//! Structural adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AnalyzeAlgorithm, AutomorphismEdge, AutomorphismGraph,
    BUILTIN_REVIEW, DyadEdge, HashMap, HashSet, PlanarityEdge, RustAlgorithm, TRIAD_NAMES,
    TransitivityEdge, TriadEdge, TriangleEdge, count_automorphisms, dyad_census, is_planar,
    low_link, transitivity, triad_census, triangle_count,
};

pub(super) struct ArticulationPoints;
pub(super) struct Bridges;

pub(super) struct TriangleCount;
pub(super) struct Transitivity;
pub(super) struct TriadCensus;
pub(super) struct DyadCensus;

pub(super) struct IsPlanar;

pub(super) struct CountAutomorphisms {
    pub(super) directed: bool,
}

impl RustAlgorithm for CountAutomorphisms {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms),
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
        control.check_output_rows(1)?;
        let mut nodes = Vec::new();
        nodes
            .try_reserve_exact(graph.node_ids().len())
            .map_err(|_| automorphism_allocation("node projection"))?;
        let adjacency_entries = usize::try_from(graph.edge_entry_count())
            .map_err(|_| automorphism_allocation("stored-edge projection"))?;
        let mut edges = Vec::new();
        edges
            .try_reserve_exact(adjacency_entries)
            .map_err(|_| automorphism_allocation("stored-edge projection"))?;
        let mut projected = 0_usize;
        for &source_id in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            } else {
                control.check_cancelled()?;
            }
            projected = projected.saturating_add(1);
            let source = automorphism_node_uuid(graph, source_id)?;
            nodes.push(source);
            for edge in graph.neighbors(source_id) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                projected = projected.saturating_add(1);
                let target = automorphism_node_uuid(graph, edge.neighbor_id)?;
                let (source, target) = if self.directed || source <= target {
                    (source, target)
                } else {
                    (target, source)
                };
                edges.push(AutomorphismEdge {
                    edge: edge.edge_uuid,
                    source,
                    target,
                });
            }
        }
        edges.sort_unstable_by_key(|edge| edge.edge);
        for duplicate in edges.windows(2).filter(|pair| pair[0].edge == pair[1].edge) {
            if duplicate[0] != duplicate[1] {
                return Err(AlgorithmError::Execution {
                    message: "automorphism edge UUID has inconsistent adjacency entries".into(),
                });
            }
        }
        edges.dedup_by_key(|edge| edge.edge);
        let graph = AutomorphismGraph::try_new(&nodes, &edges, self.directed, control)?;
        let count = count_automorphisms(&graph, control)?;
        AlgorithmOutput::from_rows(
            self.capability().algorithm,
            control,
            vec![vec![AlgorithmValue::UInt64(count)]],
        )
    }
}

pub(super) fn automorphism_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "automorphism node has no UUID identity".into(),
        })
}

pub(super) fn automorphism_allocation(context: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: format!("automorphism {context} allocation failed"),
    }
}

impl RustAlgorithm for TriangleCount {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::TriangleCount),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_output_rows(1)?;
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = graph
                .node_uuid(source)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "triangle_count node has no UUID identity".into(),
                })?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(TriangleEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                        AlgorithmError::Execution {
                            message: "triangle_count node has no UUID identity".into(),
                        }
                    })?,
                });
            }
        }
        let count = triangle_count(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::TriangleCount),
            control,
            vec![vec![AlgorithmValue::UInt64(count)]],
        )
    }
}

impl RustAlgorithm for Transitivity {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::Transitivity),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_output_rows(1)?;
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = graph
                .node_uuid(source)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "transitivity node has no UUID identity".into(),
                })?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(TransitivityEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                        AlgorithmError::Execution {
                            message: "transitivity node has no UUID identity".into(),
                        }
                    })?,
                });
            }
        }
        let value = transitivity(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::Transitivity),
            control,
            vec![vec![AlgorithmValue::Float64(value)]],
        )
    }
}

impl RustAlgorithm for IsPlanar {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::IsPlanar),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_output_rows(1)?;
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = graph
                .node_uuid(source)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "is_planar node has no UUID identity".into(),
                })?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(PlanarityEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                        AlgorithmError::Execution {
                            message: "is_planar node has no UUID identity".into(),
                        }
                    })?,
                });
            }
        }
        let value = is_planar(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::IsPlanar),
            control,
            vec![vec![AlgorithmValue::Boolean(value)]],
        )
    }
}

impl RustAlgorithm for TriadCensus {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::TriadCensus),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_output_rows(16)?;
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = graph
                .node_uuid(source)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "triad_census node has no UUID identity".into(),
                })?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(TriadEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                        AlgorithmError::Execution {
                            message: "triad_census node has no UUID identity".into(),
                        }
                    })?,
                });
            }
        }
        let counts = triad_census(&nodes, &edges, control)?;
        let rows: Vec<Vec<AlgorithmValue>> = TRIAD_NAMES
            .iter()
            .zip(counts)
            .map(|(name, count)| {
                vec![
                    AlgorithmValue::Utf8((*name).to_owned()),
                    AlgorithmValue::UInt64(count),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::TriadCensus),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for DyadCensus {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_output_rows(3)?;
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = graph
                .node_uuid(source)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "dyad_census node has no UUID identity".into(),
                })?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(DyadEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                        AlgorithmError::Execution {
                            message: "dyad_census node has no UUID identity".into(),
                        }
                    })?,
                });
            }
        }
        let counts = dyad_census(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus),
            control,
            vec![
                vec![
                    AlgorithmValue::Utf8("mutual".into()),
                    AlgorithmValue::UInt64(counts.mutual),
                ],
                vec![
                    AlgorithmValue::Utf8("asymmetric".into()),
                    AlgorithmValue::UInt64(counts.asymmetric),
                ],
                vec![
                    AlgorithmValue::Utf8("null".into()),
                    AlgorithmValue::UInt64(counts.null),
                ],
            ],
        )
    }
}

impl RustAlgorithm for Bridges {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::Bridges),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let bridges = low_link(graph, control)?
            .bridge_edges
            .into_iter()
            .collect::<HashSet<_>>();
        let mut edges = HashMap::with_capacity(bridges.len());
        for source in graph.node_ids() {
            let source_uuid = bridge_node_uuid(graph, *source)?;
            for edge in graph.neighbors(*source) {
                if !bridges.contains(&edge.edge_id) {
                    continue;
                }
                let target_uuid = bridge_node_uuid(graph, edge.neighbor_id)?;
                let (source_uuid, target_uuid) = if source_uuid < target_uuid {
                    (source_uuid, target_uuid)
                } else {
                    (target_uuid, source_uuid)
                };
                edges
                    .entry(edge.edge_id)
                    .or_insert((edge.edge_uuid, source_uuid, target_uuid));
            }
        }
        let mut edges = edges.into_values().collect::<Vec<_>>();
        edges.sort_unstable_by_key(|&(edge_uuid, source_uuid, target_uuid)| {
            (source_uuid, target_uuid, edge_uuid)
        });
        let rows: Vec<Vec<AlgorithmValue>> = edges
            .into_iter()
            .map(|(edge_uuid, source_uuid, target_uuid)| {
                vec![
                    AlgorithmValue::Uuid(edge_uuid),
                    AlgorithmValue::Uuid(source_uuid),
                    AlgorithmValue::Uuid(target_uuid),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(Algorithm::Analyze(AnalyzeAlgorithm::Bridges), control, rows)
    }
}

impl RustAlgorithm for ArticulationPoints {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::ArticulationPoints),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let result = low_link(graph, control)?;
        let rows: Vec<Vec<AlgorithmValue>> = result
            .articulation_nodes
            .into_iter()
            .map(|node| {
                graph
                    .node_uuid(node)
                    .map(|uuid| vec![AlgorithmValue::Uuid(uuid)])
                    .ok_or_else(|| AlgorithmError::Execution {
                        message: "articulation_points node has no UUID identity".into(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::ArticulationPoints),
            control,
            rows,
        )
    }
}

pub(super) fn bridge_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "bridges endpoint has no UUID identity".into(),
        })
}

#[cfg(test)]
mod tests;
