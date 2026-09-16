//! Coloring adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AnalyzeAlgorithm, BUILTIN_REVIEW, ChromaticEdge,
    EdgeColoringEdge, NodeColoringEdge, RustAlgorithm, exact_chromatic_number,
    greedy_edge_coloring, greedy_node_coloring, k1_coloring,
};

pub(super) struct K1Coloring;
pub(super) struct NodeColoring;
pub(super) struct ChromaticNumber;

pub(super) struct EdgeColoring;

impl RustAlgorithm for ChromaticNumber {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = chromatic_number_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(ChromaticEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: chromatic_number_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let value = exact_chromatic_number(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber),
            control,
            vec![vec![AlgorithmValue::UInt64(value)]],
        )
    }
}

impl RustAlgorithm for NodeColoring {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = node_coloring_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(NodeColoringEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: node_coloring_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = greedy_node_coloring(&nodes, &edges, control)?
            .into_iter()
            .map(|entry| {
                vec![
                    AlgorithmValue::Uuid(entry.node),
                    AlgorithmValue::UInt64(entry.color),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for K1Coloring {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = k1_coloring_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(NodeColoringEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: k1_coloring_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = k1_coloring(&nodes, &edges, control)?
            .into_iter()
            .map(|entry| {
                vec![
                    AlgorithmValue::Uuid(entry.node),
                    AlgorithmValue::UInt64(entry.color),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for EdgeColoring {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::EdgeColoring),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut nodes = Vec::with_capacity(graph.node_ids().len());
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            projected = projected.saturating_add(1);
            let source_uuid = edge_coloring_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(EdgeColoringEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: edge_coloring_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = greedy_edge_coloring(&nodes, &edges, control)?
            .into_iter()
            .map(|color| {
                vec![
                    AlgorithmValue::Uuid(color.edge),
                    AlgorithmValue::UInt64(color.color),
                ]
            })
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::EdgeColoring),
            control,
            rows,
        )
    }
}

pub(super) fn node_coloring_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "node_coloring node has no UUID identity".into(),
        })
}

pub(super) fn k1_coloring_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "k1_coloring node has no UUID identity".into(),
        })
}

pub(super) fn chromatic_number_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "chromatic_number node has no UUID identity".into(),
        })
}

pub(super) fn edge_coloring_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "edge_coloring node has no UUID identity".into(),
        })
}

#[cfg(test)]
mod tests;
