//! Paths cycles dag adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AnalyzeAlgorithm, BUILTIN_REVIEW, CycleEdge,
    DagLongestPathEdge, EulerCircuitEdge, EulerEdge, EulerPathEdge, EulerProjection,
    EulerTrailKind, EulerTrailOutcome, HashMap, RustAlgorithm, SpanningEdge, VecDeque,
    WeightedDagEdge, WeightedEdge, dag_longest_path, find_cycles, has_euler_circuit,
    has_euler_path, minimum_k_spanning_trees, spanning_forest, stable_dag_topology,
    weighted_dag_longest_path,
};

pub(super) struct IsDag {
    pub(super) directed: bool,
}

pub(super) struct SpanningTree {
    pub(super) algorithm: AnalyzeAlgorithm,
    pub(super) maximize: bool,
}
pub(super) struct MinimumKSpanningTree {
    pub(super) k: usize,
}

pub(super) struct TopologicalSort;

pub(super) struct DagLongestPath;
pub(super) struct WeightedDagLongestPath;

pub(super) struct EulerConstruction {
    pub(super) algorithm: AnalyzeAlgorithm,
    pub(super) directed: bool,
}
pub(super) struct FindCycles {
    pub(super) directed: bool,
}
pub(super) struct HasEulerCircuit {
    pub(super) directed: bool,
}
pub(super) struct HasEulerPath {
    pub(super) directed: bool,
}

impl RustAlgorithm for EulerConstruction {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(self.algorithm),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let projection = project_euler_graph(graph, self.directed, control)?;
        let kind = match self.algorithm {
            AnalyzeAlgorithm::EulerCircuit => EulerTrailKind::Circuit,
            AnalyzeAlgorithm::EulerPath => EulerTrailKind::Path,
            _ => unreachable!("Euler construction only registers Euler algorithms"),
        };
        let rows: Vec<Vec<AlgorithmValue>> = match projection.trail(kind, control)? {
            EulerTrailOutcome::EmptySelection => Vec::new(),
            EulerTrailOutcome::Trail(trail) => vec![vec![
                AlgorithmValue::UuidList(trail.node_path),
                AlgorithmValue::UuidList(trail.edge_path),
            ]],
        };
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

fn project_euler_graph(
    graph: &AdjacencyGraph,
    directed: bool,
    control: &AlgorithmControl,
) -> Result<EulerProjection, AlgorithmError> {
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(graph.node_ids().len())
        .map_err(|_| AlgorithmError::Execution {
            message: "Euler node projection allocation failed".into(),
        })?;
    let edge_entries =
        usize::try_from(graph.edge_entry_count()).map_err(|_| AlgorithmError::Execution {
            message: "Euler stored-edge projection exceeds platform range".into(),
        })?;
    let mut edges = Vec::new();
    edges
        .try_reserve_exact(edge_entries)
        .map_err(|_| AlgorithmError::Execution {
            message: "Euler stored-edge projection allocation failed".into(),
        })?;
    let mut projected = 0_usize;
    for &source_id in graph.node_ids() {
        if projected.is_multiple_of(4_096) {
            control.checkpoint()?;
        } else {
            control.check_cancelled()?;
        }
        projected = projected.saturating_add(1);
        let source = euler_node_uuid(graph, source_id)?;
        nodes.push(source);
        for edge in graph.neighbors(source_id) {
            if projected.is_multiple_of(4_096) {
                control.checkpoint()?;
            } else {
                control.check_cancelled()?;
            }
            projected = projected.saturating_add(1);
            edges.push(EulerEdge {
                edge: edge.edge_uuid,
                source,
                target: euler_node_uuid(graph, edge.neighbor_id)?,
            });
        }
    }
    EulerProjection::new(&nodes, &edges, directed, control)
}

pub(super) fn euler_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "Euler node has no UUID identity".into(),
        })
}

impl RustAlgorithm for HasEulerPath {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::HasEulerPath),
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
            let source_uuid = euler_path_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(EulerPathEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: euler_path_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let value = has_euler_path(&nodes, &edges, self.directed, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::HasEulerPath),
            control,
            vec![vec![AlgorithmValue::Boolean(value)]],
        )
    }
}

impl RustAlgorithm for HasEulerCircuit {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::HasEulerCircuit),
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
            let source_uuid = euler_circuit_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(EulerCircuitEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: euler_circuit_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let value = has_euler_circuit(&nodes, &edges, self.directed, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::HasEulerCircuit),
            control,
            vec![vec![AlgorithmValue::Boolean(value)]],
        )
    }
}

impl RustAlgorithm for FindCycles {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::FindCycles),
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
            let source_uuid = find_cycles_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(CycleEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: find_cycles_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let rows: Vec<Vec<AlgorithmValue>> = find_cycles(&nodes, &edges, self.directed, control)?
            .into_iter()
            .map(|cycle| vec![AlgorithmValue::UuidList(cycle)])
            .collect();
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::FindCycles),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for DagLongestPath {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPath),
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
            let source_uuid = dag_longest_path_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(DagLongestPathEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: dag_longest_path_node_uuid(graph, edge.neighbor_id)?,
                });
            }
        }
        let result = dag_longest_path(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPath),
            control,
            vec![vec![
                AlgorithmValue::Float64(result.cost),
                AlgorithmValue::UuidList(result.path),
            ]],
        )
    }
}

impl RustAlgorithm for WeightedDagLongestPath {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPathWeighted),
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
            let source_uuid = weighted_dag_longest_path_node_uuid(graph, source)?;
            nodes.push(source_uuid);
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(WeightedDagEdge {
                    edge: edge.edge_uuid,
                    source: source_uuid,
                    target: weighted_dag_longest_path_node_uuid(graph, edge.neighbor_id)?,
                    weight: edge.weight,
                });
            }
        }
        let result = weighted_dag_longest_path(&nodes, &edges, control)?;
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPathWeighted),
            control,
            vec![vec![
                AlgorithmValue::Float64(result.cost),
                AlgorithmValue::UuidList(result.path),
            ]],
        )
    }
}

impl RustAlgorithm for SpanningTree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(self.algorithm),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            let source_uuid = spanning_node_uuid(graph, source, self.algorithm)?;
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                projected = projected.saturating_add(1);
                edges.push(SpanningEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid,
                    target_uuid: spanning_node_uuid(graph, edge.neighbor_id, self.algorithm)?,
                    weight: edge.weight,
                });
            }
        }
        let forest = spanning_forest(&nodes, &edges, self.maximize, control)?;
        let mut rows = Vec::with_capacity(forest.len());
        for (index, edge) in forest.into_iter().enumerate() {
            if index.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            rows.push(vec![
                AlgorithmValue::Uuid(edge.edge_uuid),
                AlgorithmValue::Uuid(edge.source_uuid),
                AlgorithmValue::Uuid(edge.target_uuid),
                AlgorithmValue::Float64(edge.weight),
            ]);
        }
        AlgorithmOutput::from_rows(Algorithm::Analyze(self.algorithm), control, rows)
    }
}

impl RustAlgorithm for MinimumKSpanningTree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = AnalyzeAlgorithm::MinimumKSpanningTree;
        let nodes = graph.node_uuids().collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut projected = 0_usize;
        for &source in graph.node_ids() {
            let source_uuid = spanning_node_uuid(graph, source, algorithm)?;
            for edge in graph.neighbors(source) {
                if projected.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                projected = projected.saturating_add(1);
                edges.push(WeightedEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid,
                    target_uuid: spanning_node_uuid(graph, edge.neighbor_id, algorithm)?,
                    weight: edge.weight,
                });
            }
        }

        let trees = minimum_k_spanning_trees(&nodes, &edges, self.k, control)?;
        let row_count = trees
            .iter()
            .try_fold(0_usize, |count, tree| count.checked_add(tree.edges.len()))
            .ok_or_else(|| AlgorithmError::Execution {
                message: "minimum-k spanning-tree row count exceeds platform range".into(),
            })?;
        let mut rows = Vec::with_capacity(row_count);
        let mut shaped = 0_usize;
        for (tree_id, tree) in trees.into_iter().enumerate() {
            let tree_id = u64::try_from(tree_id).map_err(|_| AlgorithmError::Execution {
                message: "minimum-k spanning-tree ordinal exceeds UInt64".into(),
            })?;
            for edge in tree.edges {
                if shaped.is_multiple_of(4_096) {
                    control.checkpoint()?;
                } else {
                    control.check_cancelled()?;
                }
                shaped = shaped.saturating_add(1);
                rows.push(vec![
                    AlgorithmValue::UInt64(tree_id),
                    AlgorithmValue::Uuid(edge.edge_uuid),
                    AlgorithmValue::Uuid(edge.source_uuid),
                    AlgorithmValue::Uuid(edge.target_uuid),
                    AlgorithmValue::Float64(edge.weight),
                ]);
            }
        }
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

impl RustAlgorithm for IsDag {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::IsDag),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let is_dag = if self.directed {
            directed_is_dag(graph, control)?
        } else {
            false
        };
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::IsDag),
            control,
            vec![vec![AlgorithmValue::Boolean(is_dag)]],
        )
    }
}

impl RustAlgorithm for TopologicalSort {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Analyze(AnalyzeAlgorithm::TopologicalSort),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let topology = stable_dag_topology(graph, control)?;
        let mut rows = Vec::with_capacity(topology.order.len());
        for node in topology.order {
            let order = u64::try_from(topology.positions[&node]).map_err(|_| {
                AlgorithmError::Execution {
                    message: "topological_sort order exceeds UInt64 range".into(),
                }
            })?;
            rows.push(vec![
                AlgorithmValue::Uuid(topological_node_uuid(graph, node)?),
                AlgorithmValue::UInt64(order),
            ]);
        }
        AlgorithmOutput::from_rows(
            Algorithm::Analyze(AnalyzeAlgorithm::TopologicalSort),
            control,
            rows,
        )
    }
}

pub(super) fn directed_is_dag(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    let mut indegree: HashMap<u64, usize> = graph
        .node_ids()
        .iter()
        .copied()
        .map(|node| (node, 0))
        .collect();
    let mut visited_edges = 0_usize;
    for &node in graph.node_ids() {
        for edge in graph.neighbors(node) {
            if visited_edges.is_multiple_of(16_384) {
                control.checkpoint()?;
            }
            visited_edges += 1;
            let degree =
                indegree
                    .get_mut(&edge.neighbor_id)
                    .ok_or_else(|| AlgorithmError::Execution {
                        message: "adjacency references an unselected node".into(),
                    })?;
            *degree = degree
                .checked_add(1)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "is_dag indegree exceeds platform range".into(),
                })?;
        }
    }

    let mut ready: VecDeque<u64> = graph
        .node_ids()
        .iter()
        .copied()
        .filter(|node| indegree[node] == 0)
        .collect();
    let mut visited_nodes = 0_usize;
    while let Some(node) = ready.pop_front() {
        if visited_nodes.is_multiple_of(16_384) {
            control.checkpoint()?;
        }
        visited_nodes += 1;
        for edge in graph.neighbors(node) {
            let degree = indegree
                .get_mut(&edge.neighbor_id)
                .expect("selected adjacency target has an indegree");
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(edge.neighbor_id);
            }
        }
    }
    Ok(visited_nodes == graph.node_ids().len())
}

pub(super) fn spanning_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
    algorithm: AnalyzeAlgorithm,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: format!("{algorithm} node has no UUID identity"),
        })
}

pub(super) fn topological_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "topological_sort node has no UUID identity".into(),
        })
}

pub(super) fn find_cycles_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "find_cycles node has no UUID identity".into(),
        })
}

pub(super) fn dag_longest_path_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "dag_longest_path node has no UUID identity".into(),
        })
}

pub(super) fn weighted_dag_longest_path_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "dag_longest_path_weighted node has no UUID identity".into(),
        })
}

pub(super) fn euler_circuit_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "has_euler_circuit node has no UUID identity".into(),
        })
}

pub(super) fn euler_path_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "has_euler_path node has no UUID identity".into(),
        })
}

#[cfg(test)]
mod tests;
