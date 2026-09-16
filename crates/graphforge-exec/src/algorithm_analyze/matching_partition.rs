//! Matching partition adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AnalyzeAlgorithm, BTreeMap, BUILTIN_REVIEW, BipartiteEdge,
    ConductanceEdge, MatchingEdge, ModularityEdge, ResolvedPartitionMap, RustAlgorithm,
    WeightedEdge, conductance, maximum_bipartite_matching, maximum_cardinality_matching,
    modularity, modularity_output, normalize_weighted_undirected, resolve_bipartite_projection,
    solve_exact_matching,
};

pub(super) struct Conductance {
    pub(super) partitions: ResolvedPartitionMap,
}
pub(super) struct Modularity {
    pub(super) partitions: ResolvedPartitionMap,
}
pub(super) struct MaxBipartiteMatching {
    pub(super) partitions: Option<ResolvedPartitionMap>,
}
pub(super) struct MaxCardinalityMatching;
pub(super) struct MaxWeightMatching;

impl RustAlgorithm for MaxCardinalityMatching {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut work = 0_usize;
        for &source_id in graph.node_ids() {
            let source = cardinality_matching_node_uuid(graph, source_id)?;
            for edge in graph.neighbors(source_id) {
                if work.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                work = work.saturating_add(1);
                edges.push(MatchingEdge {
                    edge: edge.edge_uuid,
                    source,
                    target: cardinality_matching_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = maximum_cardinality_matching(&nodes, &edges, control)?
            .into_iter()
            .map(|edge| {
                vec![
                    AlgorithmValue::Uuid(edge.edge),
                    AlgorithmValue::Uuid(edge.source),
                    AlgorithmValue::Uuid(edge.target),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

impl RustAlgorithm for MaxWeightMatching {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::MaxWeightMatching),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut work = 0_usize;
        for &source_id in graph.node_ids() {
            let source_uuid = matching_node_uuid(graph, source_id)?;
            for edge in graph.neighbors(source_id) {
                if work.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                work = work.saturating_add(1);
                edges.push(WeightedEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid,
                    target_uuid: matching_node_uuid(graph, edge.neighbor_id)?,
                    weight: edge.weight,
                });
            }
        }
        let graph = normalize_weighted_undirected(&nodes, &edges, control, &mut work)?;
        let rows: Vec<Vec<AlgorithmValue>> = solve_exact_matching(&graph, control)?
            .into_iter()
            .map(|edge| {
                vec![
                    AlgorithmValue::Uuid(edge.edge_uuid),
                    AlgorithmValue::Uuid(edge.source_uuid),
                    AlgorithmValue::Uuid(edge.target_uuid),
                    AlgorithmValue::Float64(edge.weight),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

impl RustAlgorithm for Conductance {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::Conductance),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = BTreeMap::new();
        let mut scanned = 0_usize;
        for &source_id in graph.node_ids() {
            let source = conductance_node_uuid(graph, source_id)?;
            for edge in graph.neighbors(source_id) {
                if scanned.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                scanned = scanned.saturating_add(1);
                let target = conductance_node_uuid(graph, edge.neighbor_id)?;
                let (source_uuid, target_uuid) = if source <= target {
                    (source, target)
                } else {
                    (target, source)
                };
                let projected = ConductanceEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid,
                    target_uuid,
                    weight: edge.weight,
                };
                if let Some(previous) = edges.insert(edge.edge_uuid, projected)
                    && previous != projected
                {
                    return Err(AlgorithmError::Execution {
                        message: "one edge UUID identifies inconsistent conductance data".into(),
                    });
                }
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = conductance(
            &nodes,
            &edges.into_values().collect::<Vec<_>>(),
            graph.is_directed(),
            &self.partitions,
            control,
        )?
        .into_iter()
        .map(|row| {
            vec![
                AlgorithmValue::Utf8(row.partition_id),
                AlgorithmValue::Float64(row.conductance),
            ]
        })
        .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::Conductance),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for Modularity {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::Modularity),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = BTreeMap::new();
        let mut scanned = 0_usize;
        for &source_id in graph.node_ids() {
            let source = partition_metric_node_uuid(graph, source_id, "modularity")?;
            for edge in graph.neighbors(source_id) {
                if scanned.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                scanned = scanned.saturating_add(1);
                let target = partition_metric_node_uuid(graph, edge.neighbor_id, "modularity")?;
                let projected = ModularityEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid: source,
                    target_uuid: target,
                    weight: edge.weight,
                };
                if let Some(previous) = edges.insert(edge.edge_uuid, projected)
                    && previous != projected
                    && previous
                        != (ModularityEdge {
                            source_uuid: target,
                            target_uuid: source,
                            ..projected
                        })
                {
                    return Err(AlgorithmError::Execution {
                        message: "one edge UUID identifies inconsistent modularity data".into(),
                    });
                }
            }
        }
        let value = modularity(
            &nodes,
            &edges.into_values().collect::<Vec<_>>(),
            graph.is_directed(),
            &self.partitions,
            control,
        )?;
        modularity_output(value)
    }
}

impl RustAlgorithm for MaxBipartiteMatching {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = BTreeMap::new();
        let mut scanned = 0_usize;
        for &source_id in graph.node_ids() {
            let source = bipartite_node_uuid(graph, source_id)?;
            for edge in graph.neighbors(source_id) {
                if scanned.is_multiple_of(4_096) {
                    control.check_cancelled()?;
                }
                scanned = scanned.saturating_add(1);
                let target = bipartite_node_uuid(graph, edge.neighbor_id)?;
                let endpoints = if source <= target {
                    (source, target)
                } else {
                    (target, source)
                };
                if let Some(previous) = edges.insert(
                    edge.edge_uuid,
                    BipartiteEdge {
                        edge: edge.edge_uuid,
                        source: endpoints.0,
                        target: endpoints.1,
                    },
                ) && (previous.source, previous.target) != endpoints
                {
                    return Err(AlgorithmError::Execution {
                        message: "one edge UUID identifies multiple endpoint pairs".into(),
                    });
                }
            }
        }
        let projection = resolve_bipartite_projection(
            &nodes,
            &edges.into_values().collect::<Vec<_>>(),
            self.partitions.as_ref(),
            control,
        )?;
        let rows: Vec<Vec<AlgorithmValue>> = maximum_bipartite_matching(&projection, control)?
            .into_iter()
            .map(|edge| {
                vec![
                    AlgorithmValue::Uuid(edge.edge),
                    AlgorithmValue::Uuid(edge.source),
                    AlgorithmValue::Uuid(edge.target),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching),
            control,
            rows,
        )
    }
}

pub(super) fn bipartite_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_bipartite_matching node has no UUID identity".into(),
        })
}

pub(super) fn matching_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_weight_matching node has no UUID identity".into(),
        })
}

pub(super) fn cardinality_matching_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_cardinality_matching node has no UUID identity".into(),
        })
}

pub(super) fn conductance_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    partition_metric_node_uuid(graph, node, "conductance")
}

pub(super) fn partition_metric_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
    algorithm: &str,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: format!("{algorithm} node has no UUID identity"),
        })
}

#[cfg(test)]
mod tests;
