//! Rust-owned path handlers registered under the shared algorithm dispatch contract.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, PathAlgorithm};
use graphforge_core::{GfError, OntologyMode, PathsOptions};
use graphforge_ir::Direction;
use sha2::{Digest, Sha256};

use crate::AdjacencyProvider;
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_graph::{
    AdjacencyGraph, AdjacencySelection, export_adjacency, load_node_numeric_property,
};
use crate::algorithm_output::shape_algorithm_output;
use crate::algorithm_paths_astar::exact_astar;
use crate::algorithm_paths_bellman_ford::exact_bellman_ford;
use crate::algorithm_paths_delta_stepping::exact_delta_stepping;
use crate::algorithm_paths_dfs::depth_first_search;
use crate::algorithm_paths_dijkstra::{exact_dijkstra, exact_dijkstra_all_pairs};
use crate::algorithm_paths_floyd_warshall::exact_floyd_warshall;
use crate::algorithm_paths_gomory_hu::gomory_hu_forest;
use crate::algorithm_paths_max_flow::{CapacityEdge, maximum_flow};
use crate::algorithm_paths_min_cost_flow::{
    CostCapacityEdge, min_cost_flow_adjacency_entries, minimum_cost_maximum_flow,
    shape_min_cost_flow_output,
};
use crate::algorithm_paths_min_cut::minimum_cut;
use crate::algorithm_paths_min_steiner::minimum_steiner_tree;
use crate::algorithm_paths_prize_steiner::{
    NodePrize, PrizeSteinerInputEdge, ResolvedNumber, prize_collecting_steiner_tree,
};
use crate::algorithm_paths_random_walk::{RandomWalkAdjacencySource, RandomWalkEdge, random_walks};
use crate::algorithm_paths_steiner::{SteinerKind, normalize_steiner_invocation};
use crate::algorithm_paths_transitive_closure::positive_transitive_closure;
use crate::algorithm_paths_yens::exact_yens;
use crate::algorithm_weighted_undirected::WeightedEdge;

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "hop count then topology order; topology-ordered predecessor ties",
    platforms: "Rust workspace targets",
};

struct Bfs {
    source: [u8; 16],
    target: Option<[u8; 16]>,
}

struct Dfs {
    source: [u8; 16],
}

struct Dijkstra {
    source: [u8; 16],
    target: Option<[u8; 16]>,
}

struct DijkstraAllPairs {
    source: [u8; 16],
}

struct AStar {
    source: [u8; 16],
    target: Option<[u8; 16]>,
    heuristic: Option<HashMap<u64, f64>>,
}

struct BellmanFord {
    source: [u8; 16],
    target: Option<[u8; 16]>,
}

struct DeltaStepping {
    source: [u8; 16],
    target: Option<[u8; 16]>,
}

impl RustAlgorithm for DeltaStepping {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::DeltaStepping),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("delta_stepping source UUID is not in the selected graph"))?;
        let target = self
            .target
            .map(|uuid| {
                graph.node_id(&uuid).ok_or_else(|| {
                    execution("delta_stepping target UUID is not in the selected graph")
                })
            })
            .transpose()?;
        let rows: Vec<Vec<AlgorithmValue>> = exact_delta_stepping(graph, source, target, control)?
            .into_iter()
            .map(|result| {
                Ok(vec![
                    AlgorithmValue::Uuid(self.source),
                    AlgorithmValue::Uuid(node_uuid(graph, result.target)?),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(
            Algorithm::Paths(PathAlgorithm::DeltaStepping),
            control,
            rows,
        )
    }
}

struct FloydWarshall {
    source: [u8; 16],
}

struct Yens {
    source: [u8; 16],
    target: Option<[u8; 16]>,
    k: usize,
}

struct TransitiveClosure;

struct MaxFlow {
    source: [u8; 16],
    target: Option<[u8; 16]>,
    edges: bool,
}

struct MinCut {
    source: [u8; 16],
    target: Option<[u8; 16]>,
    edges: bool,
}

struct GomoryHuTree;

impl RustAlgorithm for GomoryHuTree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::GomoryHuTree),
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
        let rows: Vec<Vec<AlgorithmValue>> = gomory_hu_forest(
            &graph.node_uuids().collect::<Vec<_>>(),
            &capacity_edges(graph)?,
            graph.is_directed(),
            control,
        )?
        .into_iter()
        .map(|edge| {
            vec![
                AlgorithmValue::Uuid(edge.source_uuid),
                AlgorithmValue::Uuid(edge.target_uuid),
                AlgorithmValue::Float64(edge.cut_value),
            ]
        })
        .collect();
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

struct MinCostFlow {
    source: [u8; 16],
    target: Option<[u8; 16]>,
    edges: bool,
    input: Arc<[CostCapacityEdge]>,
}

struct RandomWalk {
    source: [u8; 16],
    k: usize,
    walk_length: usize,
    seed: u64,
    weighted: bool,
}

struct MinSteinerTree {
    terminals: Arc<[[u8; 16]]>,
}

struct PrizeCollectingSteinerTree {
    terminals: Arc<[[u8; 16]]>,
    prizes: Arc<[NodePrize]>,
}

impl RustAlgorithm for PrizeCollectingSteinerTree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::PrizeCollectingSteinerTree),
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
        let edges = capacity_edges(graph)?
            .into_iter()
            .map(|edge| PrizeSteinerInputEdge {
                edge_uuid: edge.edge_uuid,
                source_uuid: edge.source_uuid,
                target_uuid: edge.target_uuid,
                cost: ResolvedNumber::Float64(edge.capacity),
            })
            .collect::<Vec<_>>();
        let rows: Vec<Vec<AlgorithmValue>> = prize_collecting_steiner_tree(
            &nodes,
            &self.prizes,
            &edges,
            &self.terminals,
            graph.is_directed(),
            control,
        )?
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

impl RustAlgorithm for MinSteinerTree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::MinSteinerTree),
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
        let solution = minimum_steiner_tree(
            &nodes,
            &weighted_undirected_edges(graph)?,
            &self.terminals,
            control,
        )?;
        let rows: Vec<Vec<AlgorithmValue>> = solution
            .edges
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

struct GraphRandomWalkAdjacency<'a>(&'a AdjacencyGraph);

impl RandomWalkAdjacencySource for GraphRandomWalkAdjacency<'_> {
    fn choices(&self, node: &[u8; 16]) -> Result<Vec<RandomWalkEdge>, AlgorithmError> {
        let Some(node_id) = self.0.node_id(node) else {
            return Ok(Vec::new());
        };
        self.0
            .neighbors(node_id)
            .iter()
            .map(|edge| {
                Ok(RandomWalkEdge {
                    edge_uuid: edge.edge_uuid,
                    neighbor_uuid: node_uuid(self.0, edge.neighbor_id)?,
                    weight: edge.weight,
                })
            })
            .collect()
    }
}

impl RustAlgorithm for RandomWalk {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::RandomWalk),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        if graph.node_id(&self.source).is_none() {
            return Err(execution(
                "random_walk source UUID is not in the selected graph",
            ));
        }
        let rows: Vec<Vec<AlgorithmValue>> = random_walks(
            &GraphRandomWalkAdjacency(graph),
            &[self.source],
            self.k,
            self.walk_length,
            self.seed,
            self.weighted,
            control,
        )?
        .into_iter()
        .map(|walk| {
            vec![
                AlgorithmValue::Uuid(self.source),
                AlgorithmValue::UuidList(walk),
            ]
        })
        .collect();
        AlgorithmOutput::from_rows(self.capability().algorithm, control, rows)
    }
}

impl RustAlgorithm for MaxFlow {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(if self.edges {
                PathAlgorithm::MaxFlowEdges
            } else {
                PathAlgorithm::MaxFlow
            }),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let target = self
            .target
            .ok_or_else(|| execution("maximum flow requires a target selector"))?;
        let algorithm = self.capability().algorithm;
        let solution = maximum_flow(
            &graph.node_uuids().collect::<Vec<_>>(),
            &capacity_edges(graph)?,
            self.source,
            target,
            graph.is_directed(),
            control,
        )?;
        let rows: Vec<Vec<AlgorithmValue>> = if self.edges {
            solution
                .edge_flows
                .into_iter()
                .map(|(edge, flow)| {
                    vec![
                        AlgorithmValue::Uuid(edge.edge_uuid),
                        AlgorithmValue::Uuid(edge.source_uuid),
                        AlgorithmValue::Uuid(edge.target_uuid),
                        AlgorithmValue::Float64(flow),
                    ]
                })
                .collect()
        } else {
            vec![vec![
                AlgorithmValue::Uuid(self.source),
                AlgorithmValue::Uuid(target),
                AlgorithmValue::Float64(solution.value),
            ]]
        };
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for MinCut {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(if self.edges {
                PathAlgorithm::MinCutEdges
            } else {
                PathAlgorithm::MinCut
            }),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let target = self
            .target
            .ok_or_else(|| execution("minimum cut requires a target selector"))?;
        let algorithm = self.capability().algorithm;
        let solution = minimum_cut(
            &graph.node_uuids().collect::<Vec<_>>(),
            &capacity_edges(graph)?,
            self.source,
            target,
            graph.is_directed(),
            control,
        )?;
        let rows: Vec<Vec<AlgorithmValue>> = if self.edges {
            solution
                .cut_edges
                .into_iter()
                .map(|edge| {
                    vec![
                        AlgorithmValue::Uuid(edge.edge_uuid),
                        AlgorithmValue::Uuid(edge.source_uuid),
                        AlgorithmValue::Uuid(edge.target_uuid),
                        AlgorithmValue::Float64(edge.capacity),
                    ]
                })
                .collect()
        } else {
            vec![vec![
                AlgorithmValue::Uuid(self.source),
                AlgorithmValue::Uuid(target),
                AlgorithmValue::Float64(solution.value),
            ]]
        };
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for MinCostFlow {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(if self.edges {
                PathAlgorithm::MinCostMaxFlowEdges
            } else {
                PathAlgorithm::MinCostMaxFlow
            }),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let target = self
            .target
            .ok_or_else(|| execution("minimum-cost maximum flow requires a target selector"))?;
        let adjacency_entries = min_cost_flow_adjacency_entries(&self.input, graph.is_directed())?;
        control.check_graph_size(graph.node_ids().len(), adjacency_entries)?;
        control.check_cancelled()?;
        let mut node_uuids = Vec::new();
        #[cfg(test)]
        MIN_COST_NODE_PROJECTION_ATTEMPTS.with(|attempts| {
            attempts.set(attempts.get().saturating_add(1));
        });
        node_uuids
            .try_reserve_exact(graph.node_ids().len())
            .map_err(|_| {
                execution("minimum-cost maximum-flow node projection allocation failed")
            })?;
        node_uuids.extend(graph.node_uuids());
        let solution = minimum_cost_maximum_flow(
            &node_uuids,
            &self.input,
            self.source,
            target,
            graph.is_directed(),
            control,
        )?;
        shape_min_cost_flow_output(solution, self.source, target, self.edges, control)
    }
}

#[cfg(test)]
std::thread_local! {
    static MIN_COST_NODE_PROJECTION_ATTEMPTS: std::cell::Cell<u64> = const {
        std::cell::Cell::new(0)
    };
}

fn capacity_edges(graph: &AdjacencyGraph) -> Result<Vec<CapacityEdge>, AlgorithmError> {
    let mut edges = std::collections::BTreeMap::new();
    for &source_id in graph.node_ids() {
        let source_uuid = node_uuid(graph, source_id)?;
        for edge in graph.neighbors(source_id) {
            let target_uuid = node_uuid(graph, edge.neighbor_id)?;
            let (source_uuid, target_uuid) = if graph.is_directed() || source_uuid <= target_uuid {
                (source_uuid, target_uuid)
            } else {
                (target_uuid, source_uuid)
            };
            let capacity = CapacityEdge {
                edge_uuid: edge.edge_uuid,
                source_uuid,
                target_uuid,
                capacity: edge.weight,
            };
            if let Some(previous) = edges.insert(edge.edge_uuid, capacity)
                && previous != capacity
            {
                return Err(execution(
                    "capacity adjacency has conflicting rows for one edge UUID",
                ));
            }
        }
    }
    Ok(edges.into_values().collect())
}

fn weighted_undirected_edges(graph: &AdjacencyGraph) -> Result<Vec<WeightedEdge>, AlgorithmError> {
    if graph.is_directed() {
        return Err(execution(
            "minimum Steiner tree requires an undirected graph",
        ));
    }
    capacity_edges(graph).map(|edges| {
        edges
            .into_iter()
            .map(|edge| WeightedEdge {
                edge_uuid: edge.edge_uuid,
                source_uuid: edge.source_uuid,
                target_uuid: edge.target_uuid,
                weight: edge.capacity,
            })
            .collect()
    })
}

fn cost_capacity_edges(
    capacity_graph: &AdjacencyGraph,
    cost_graph: &AdjacencyGraph,
) -> Result<Vec<CostCapacityEdge>, AlgorithmError> {
    let capacities = capacity_edges(capacity_graph)?
        .into_iter()
        .map(|edge| (edge.edge_uuid, edge))
        .collect::<std::collections::BTreeMap<_, _>>();
    let costs = capacity_edges(cost_graph)?
        .into_iter()
        .map(|edge| (edge.edge_uuid, edge.capacity))
        .collect::<std::collections::BTreeMap<_, _>>();
    if capacities.len() != costs.len() || capacities.keys().ne(costs.keys()) {
        return Err(execution(
            "minimum-cost maximum-flow property projections disagree on selected edges",
        ));
    }
    capacities
        .into_values()
        .map(|edge| {
            Ok(CostCapacityEdge {
                edge_uuid: edge.edge_uuid,
                source_uuid: edge.source_uuid,
                target_uuid: edge.target_uuid,
                capacity: edge.capacity,
                unit_cost: *costs.get(&edge.edge_uuid).ok_or_else(|| {
                    execution("minimum-cost maximum-flow edge has no resolved cost")
                })?,
            })
        })
        .collect()
}

impl RustAlgorithm for TransitiveClosure {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::TransitiveClosure),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let rows: Vec<Vec<AlgorithmValue>> = positive_transitive_closure(graph, control)?
            .into_iter()
            .map(|pair| {
                Ok(vec![
                    AlgorithmValue::Uuid(node_uuid(graph, pair.source)?),
                    AlgorithmValue::Uuid(node_uuid(graph, pair.target)?),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(
            Algorithm::Paths(PathAlgorithm::TransitiveClosure),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for Yens {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::Yens),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("yens source UUID is not in the selected graph"))?;
        let target_uuid = self
            .target
            .ok_or_else(|| execution("yens requires a target selector"))?;
        let target = graph
            .node_id(&target_uuid)
            .ok_or_else(|| execution("yens target UUID is not in the selected graph"))?;
        let rows: Vec<Vec<AlgorithmValue>> = exact_yens(graph, source, target, self.k, control)?
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                let rank = u64::try_from(index.saturating_add(1))
                    .map_err(|_| execution("yens rank exceeds the UInt64 range"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(self.source),
                    AlgorithmValue::Uuid(target_uuid),
                    AlgorithmValue::UInt64(rank),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::Yens), control, rows)
    }
}

impl RustAlgorithm for FloydWarshall {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::FloydWarshall),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        if graph.node_id(&self.source).is_none() {
            return Err(execution(
                "floyd_warshall source UUID is not in the selected graph",
            ));
        }
        let rows: Vec<Vec<AlgorithmValue>> = exact_floyd_warshall(graph, control)?
            .into_iter()
            .map(|result| {
                Ok(vec![
                    AlgorithmValue::Uuid(node_uuid(graph, result.source)?),
                    AlgorithmValue::Uuid(node_uuid(graph, result.target)?),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(
            Algorithm::Paths(PathAlgorithm::FloydWarshall),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for BellmanFord {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::BellmanFord),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("bellman_ford source UUID is not in the selected graph"))?;
        let target = self
            .target
            .map(|uuid| {
                graph.node_id(&uuid).ok_or_else(|| {
                    execution("bellman_ford target UUID is not in the selected graph")
                })
            })
            .transpose()?;
        let rows: Vec<Vec<AlgorithmValue>> = exact_bellman_ford(graph, source, target, control)?
            .into_iter()
            .map(|result| {
                Ok(vec![
                    AlgorithmValue::Uuid(self.source),
                    AlgorithmValue::Uuid(node_uuid(graph, result.target)?),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::BellmanFord), control, rows)
    }
}

impl RustAlgorithm for AStar {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::AStar),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("astar source UUID is not in the selected graph"))?;
        let target_uuid = self
            .target
            .ok_or_else(|| execution("astar requires a target selector"))?;
        let target = graph
            .node_id(&target_uuid)
            .ok_or_else(|| execution("astar target UUID is not in the selected graph"))?;
        let rows: Vec<Vec<AlgorithmValue>> =
            exact_astar(graph, source, target, self.heuristic.as_ref(), control)?
                .into_iter()
                .map(|result| {
                    Ok(vec![
                        AlgorithmValue::Uuid(self.source),
                        AlgorithmValue::Uuid(target_uuid),
                        AlgorithmValue::Float64(result.cost),
                        AlgorithmValue::UuidList(
                            result
                                .nodes
                                .into_iter()
                                .map(|node| node_uuid(graph, node))
                                .collect::<Result<_, _>>()?,
                        ),
                    ])
                })
                .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::AStar), control, rows)
    }
}

impl RustAlgorithm for DijkstraAllPairs {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::DijkstraAllPairs),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        if graph.node_id(&self.source).is_none() {
            return Err(execution(
                "dijkstra_all_pairs source UUID is not in the selected graph",
            ));
        }
        let rows: Vec<Vec<AlgorithmValue>> = exact_dijkstra_all_pairs(graph, control)?
            .into_iter()
            .map(|result| {
                Ok(vec![
                    AlgorithmValue::Uuid(node_uuid(graph, result.source)?),
                    AlgorithmValue::Uuid(node_uuid(graph, result.target)?),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(
            Algorithm::Paths(PathAlgorithm::DijkstraAllPairs),
            control,
            rows,
        )
    }
}

impl RustAlgorithm for Dijkstra {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::Dijkstra),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("dijkstra source UUID is not in the selected graph"))?;
        let target = self
            .target
            .map(|uuid| {
                graph
                    .node_id(&uuid)
                    .ok_or_else(|| execution("dijkstra target UUID is not in the selected graph"))
            })
            .transpose()?;
        let rows: Vec<Vec<AlgorithmValue>> = exact_dijkstra(graph, source, target, control)?
            .into_iter()
            .map(|result| {
                Ok(vec![
                    AlgorithmValue::Uuid(self.source),
                    AlgorithmValue::Uuid(node_uuid(graph, result.target)?),
                    AlgorithmValue::Float64(result.cost),
                    AlgorithmValue::UuidList(
                        result
                            .nodes
                            .into_iter()
                            .map(|node| node_uuid(graph, node))
                            .collect::<Result<_, _>>()?,
                    ),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::Dijkstra), control, rows)
    }
}

fn node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| execution("path node has no UUID identity"))
}

impl RustAlgorithm for Bfs {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::Bfs),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("bfs source UUID is not in the selected graph"))?;
        let target = self
            .target
            .map(|uuid| {
                graph
                    .node_id(&uuid)
                    .ok_or_else(|| execution("bfs target UUID is not in the selected graph"))
            })
            .transpose()?;

        let mut queue = VecDeque::from([source]);
        let mut distance = HashMap::from([(source, 0_u32)]);
        let mut predecessor = HashMap::new();
        let mut visited = 0_usize;
        while let Some(node) = queue.pop_front() {
            if visited.is_multiple_of(4_096) {
                control.checkpoint()?;
            }
            visited += 1;
            if target == Some(node) {
                break;
            }
            let mut neighbors: Vec<_> = graph
                .neighbors(node)
                .iter()
                .map(|edge| edge.neighbor_id)
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            for neighbor in neighbors {
                if distance.contains_key(&neighbor) {
                    continue;
                }
                distance.insert(neighbor, distance[&node] + 1);
                predecessor.insert(neighbor, node);
                queue.push_back(neighbor);
            }
        }

        let targets = match target {
            Some(target) if distance.contains_key(&target) => vec![target],
            Some(_) => Vec::new(),
            None => {
                let mut targets: Vec<_> = distance.keys().copied().collect();
                targets.sort_by_key(|node| (distance[node], *node));
                targets
            }
        };
        let mut rows = Vec::with_capacity(targets.len());
        for target in targets {
            let target_uuid = graph
                .node_uuid(target)
                .ok_or_else(|| execution("bfs target has no UUID identity"))?;
            rows.push(vec![
                AlgorithmValue::Uuid(self.source),
                AlgorithmValue::Uuid(target_uuid),
                AlgorithmValue::Float64(f64::from(distance[&target])),
                AlgorithmValue::UuidList(path_uuids(graph, source, target, &predecessor)?),
            ]);
        }
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::Bfs), control, rows)
    }
}

impl RustAlgorithm for Dfs {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::Dfs),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let source = graph
            .node_id(&self.source)
            .ok_or_else(|| execution("dfs source UUID is not in the selected graph"))?;
        let rows: Vec<Vec<AlgorithmValue>> = depth_first_search(graph, source, control)?
            .into_iter()
            .map(|visit| {
                Ok(vec![
                    AlgorithmValue::Uuid(node_uuid(graph, visit.node)?),
                    AlgorithmValue::UInt64(visit.depth),
                    AlgorithmValue::UInt64(visit.order),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(Algorithm::Paths(PathAlgorithm::Dfs), control, rows)
    }
}

fn path_uuids(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    predecessor: &HashMap<u64, u64>,
) -> Result<Vec<[u8; 16]>, AlgorithmError> {
    let mut reversed = vec![target];
    let mut node = target;
    while node != source {
        node = *predecessor
            .get(&node)
            .ok_or_else(|| execution("bfs predecessor chain is incomplete"))?;
        reversed.push(node);
    }
    reversed.reverse();
    reversed
        .into_iter()
        .map(|node| {
            graph
                .node_uuid(node)
                .ok_or_else(|| execution("bfs path node has no UUID identity"))
        })
        .collect()
}

pub(crate) fn register_path_algorithms(
    registry: &mut AlgorithmRegistry,
    source: [u8; 16],
    target: Option<[u8; 16]>,
    k: usize,
    heuristic: Option<HashMap<u64, f64>>,
    min_cost_input: Option<Arc<[CostCapacityEdge]>>,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(Bfs { source, target }))?;
    registry.register(Arc::new(Dfs { source }))?;
    registry.register(Arc::new(Dijkstra { source, target }))?;
    registry.register(Arc::new(DijkstraAllPairs { source }))?;
    registry.register(Arc::new(BellmanFord { source, target }))?;
    registry.register(Arc::new(DeltaStepping { source, target }))?;
    registry.register(Arc::new(FloydWarshall { source }))?;
    registry.register(Arc::new(Yens { source, target, k }))?;
    registry.register(Arc::new(TransitiveClosure))?;
    registry.register(Arc::new(MaxFlow {
        source,
        target,
        edges: false,
    }))?;
    registry.register(Arc::new(MaxFlow {
        source,
        target,
        edges: true,
    }))?;
    registry.register(Arc::new(MinCut {
        source,
        target,
        edges: false,
    }))?;
    registry.register(Arc::new(MinCut {
        source,
        target,
        edges: true,
    }))?;
    if let Some(input) = min_cost_input {
        registry.register(Arc::new(MinCostFlow {
            source,
            target,
            edges: false,
            input: Arc::clone(&input),
        }))?;
        registry.register(Arc::new(MinCostFlow {
            source,
            target,
            edges: true,
            input,
        }))?;
    }
    registry.register(Arc::new(AStar {
        source,
        target,
        heuristic,
    }))
}

/// Execute a typed path algorithm through Rust dispatch and return its
/// canonical UUID-only Arrow batch.
///
/// # Errors
/// Returns structured validation/execution errors for malformed options,
/// unavailable algorithms, adjacency reads, limits, or result shaping.
pub fn paths_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: PathsOptions,
) -> Result<RecordBatch, GfError> {
    paths_algorithm_with_compute(
        provider,
        dir,
        mode,
        source,
        target,
        options,
        AlgorithmLimits::default(),
        None,
    )
}

/// Execute path/flow algorithms with explicit limits and optional private compute pool (#554).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors paths_algorithm plus resource-policy compute handles"
)]
pub fn paths_algorithm_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: PathsOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<RecordBatch, GfError> {
    validate_path_options(source, target, &options)?;
    let via = options.via.as_deref().unwrap_or("*");
    let graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label: None,
            via,
            direction: if options.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: if matches!(
                options.by,
                PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
            ) {
                options.capacity_property.as_deref()
            } else {
                options.weight.as_deref()
            },
        },
    )?;
    let algorithm = Algorithm::Paths(options.by);
    let mut control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        control = control.with_compute_pool(pool);
    }
    if let Some(kind) = steiner_kind(options.by) {
        return execute_steiner(&graph, dir, source, target, &options, kind, &control);
    }
    if matches!(options.by, PathAlgorithm::GomoryHuTree) {
        let mut registry = AlgorithmRegistry::default();
        registry.register(Arc::new(GomoryHuTree))?;
        let output = registry.execute(algorithm, &graph, &control)?;
        return shape_algorithm_output(algorithm, &output).map_err(Into::into);
    }
    let source = source.expect("source-based path algorithm validated");
    let PathsOptions {
        by,
        via,
        directed,
        k,
        weight,
        capacity_property: _,
        cost_property,
        heuristic,
        walk_length,
        seed,
        ..
    } = options;
    let min_cost_input = if matches!(
        by,
        PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
    ) {
        let cost_graph = export_adjacency(
            provider,
            dir,
            mode,
            AdjacencySelection {
                label: None,
                via: via.as_deref().unwrap_or("*"),
                direction: if directed {
                    Direction::Out
                } else {
                    Direction::Undirected
                },
                weight: cost_property.as_deref(),
            },
        )?;
        Some(Arc::from(cost_capacity_edges(&graph, &cost_graph)?))
    } else {
        None
    };
    let heuristic = heuristic
        .as_deref()
        .map(|property| load_node_numeric_property(&graph, dir, property))
        .transpose()?;
    let mut registry = AlgorithmRegistry::default();
    register_path_algorithms(&mut registry, source, target, k, heuristic, min_cost_input)?;
    if matches!(by, PathAlgorithm::RandomWalk) {
        let (k, walk_length, seed) = normalize_random_walk_options(target, k, walk_length, seed)?;
        registry.register(Arc::new(RandomWalk {
            source,
            k,
            walk_length,
            seed,
            weighted: weight.is_some(),
        }))?;
    }
    let output = registry.execute(algorithm, &graph, &control)?;
    shape_algorithm_output(algorithm, &output).map_err(Into::into)
}

/// Fingerprint the exact topology and graph-native values consumed by paths.
///
/// This performs projection and option validation but never runs a path kernel.
///
/// # Errors
/// Returns the same selector, property, and projection failures as [`paths_algorithm`].
pub fn paths_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
) -> Result<[u8; 32], GfError> {
    validate_path_options(source, target, options)?;
    let via = options.via.as_deref().unwrap_or("*");
    let graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label: None,
            via,
            direction: if options.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: if matches!(
                options.by,
                PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
            ) {
                options.capacity_property.as_deref()
            } else {
                options.weight.as_deref()
            },
        },
    )?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge_paths_projection_v1");
    digest.update(graph.descriptor_projection_fingerprint()?.as_bytes());

    if matches!(
        options.by,
        PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
    ) {
        let cost_graph = export_adjacency(
            provider,
            dir,
            mode,
            AdjacencySelection {
                label: None,
                via,
                direction: if options.directed {
                    Direction::Out
                } else {
                    Direction::Undirected
                },
                weight: options.cost_property.as_deref(),
            },
        )?;
        digest.update(b"cost");
        digest.update(cost_graph.descriptor_projection_fingerprint()?.as_bytes());
    }
    if let Some(property) = options.heuristic.as_deref() {
        let values = load_node_numeric_property(&graph, dir, property)?;
        update_node_numeric_projection(&mut digest, &graph, "heuristic", property, &values)?;
    }
    if let Some(property) = options.prize_property.as_deref() {
        let values = load_node_numeric_property(&graph, dir, property)?;
        update_node_numeric_projection(&mut digest, &graph, "prize", property, &values)?;
    }
    Ok(digest.finalize().into())
}

fn update_node_numeric_projection(
    digest: &mut Sha256,
    graph: &AdjacencyGraph,
    role: &str,
    property: &str,
    values: &HashMap<u64, f64>,
) -> Result<(), GfError> {
    digest.update(
        u64::try_from(role.len())
            .map_err(|_| GfError::Execution("projection role is too long".into()))?
            .to_be_bytes(),
    );
    digest.update(role.as_bytes());
    digest.update(
        u64::try_from(property.len())
            .map_err(|_| GfError::Execution("projection property name is too long".into()))?
            .to_be_bytes(),
    );
    digest.update(property.as_bytes());
    digest.update(
        u64::try_from(graph.node_ids().len())
            .map_err(|_| {
                GfError::Execution("numeric projection node count exceeds UInt64 range".into())
            })?
            .to_be_bytes(),
    );
    for node_id in graph.node_ids() {
        let uuid = graph.node_uuid(*node_id).ok_or_else(|| {
            GfError::Execution("numeric projection node has no UUID identity".into())
        })?;
        let value = values.get(node_id).ok_or_else(|| {
            GfError::Execution("numeric projection node has no property value".into())
        })?;
        if !value.is_finite() {
            return Err(GfError::Execution(
                "numeric projection contains a non-finite value".into(),
            ));
        }
        digest.update(uuid);
        digest.update(value.to_bits().to_be_bytes());
    }
    Ok(())
}

fn execute_steiner(
    graph: &AdjacencyGraph,
    dir: &Path,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
    kind: SteinerKind,
    control: &AlgorithmControl,
) -> Result<RecordBatch, GfError> {
    let mut selected_nodes = Vec::new();
    selected_nodes
        .try_reserve_exact(graph.node_ids().len())
        .map_err(|_| execution("Steiner projection allocation exceeds available memory"))?;
    for uuid in graph.node_uuids() {
        control.check_cancelled()?;
        selected_nodes.push(uuid);
    }
    let invocation =
        normalize_steiner_invocation(kind, source, target, options, &selected_nodes, control)?;
    let mut registry = AlgorithmRegistry::default();
    match kind {
        SteinerKind::MinimumTree => registry.register(Arc::new(MinSteinerTree {
            terminals: Arc::from(invocation.terminal_uuids()),
        }))?,
        SteinerKind::PrizeCollecting => {
            let property = options
                .prize_property
                .as_deref()
                .expect("prize property normalized");
            let mapping = load_node_numeric_property(graph, dir, property)?;
            let prizes = graph
                .node_ids()
                .iter()
                .map(|node_id| NodePrize {
                    node_uuid: graph
                        .node_uuid(*node_id)
                        .expect("selected node ID has a UUID"),
                    prize: ResolvedNumber::Float64(mapping[node_id]),
                })
                .collect::<Vec<_>>();
            registry.register(Arc::new(PrizeCollectingSteinerTree {
                terminals: Arc::from(invocation.terminal_uuids()),
                prizes: Arc::from(prizes),
            }))?;
        }
    }
    let algorithm = Algorithm::Paths(options.by);
    registry
        .execute(algorithm, graph, control)
        .and_then(|output| shape_algorithm_output(algorithm, &output))
        .map_err(Into::into)
}

fn validate_path_options(
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
) -> Result<(), GfError> {
    let by = options.by;
    let k = options.k;
    let via = options.via.as_deref().unwrap_or("*");
    let weight = options.weight.as_deref();
    let heuristic = options.heuristic.as_deref();
    let capacity_property = options.capacity_property.as_deref();
    let cost_property = options.cost_property.as_deref();
    let min_cost = matches!(
        by,
        PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
    );
    validate_source_and_steiner_fields(source, options)?;
    validate_gomory_hu_invocation(source, target, options)?;
    if invalid_selector(via) {
        return Err(GfError::Validation(format!(
            "invalid paths relationship selector {via:?}"
        )));
    }
    if matches!(
        by,
        PathAlgorithm::Bfs
            | PathAlgorithm::Dfs
            | PathAlgorithm::Dijkstra
            | PathAlgorithm::DijkstraAllPairs
            | PathAlgorithm::AStar
            | PathAlgorithm::BellmanFord
            | PathAlgorithm::DeltaStepping
            | PathAlgorithm::FloydWarshall
            | PathAlgorithm::TransitiveClosure
            | PathAlgorithm::MaxFlow
            | PathAlgorithm::MaxFlowEdges
            | PathAlgorithm::MinCut
            | PathAlgorithm::MinCutEdges
            | PathAlgorithm::MinCostMaxFlow
            | PathAlgorithm::MinCostMaxFlowEdges
            | PathAlgorithm::GomoryHuTree
            | PathAlgorithm::MinSteinerTree
            | PathAlgorithm::PrizeCollectingSteinerTree
    ) && k != 1
    {
        return Err(GfError::Validation(format!("{by} k must be 1")));
    }
    if matches!(
        by,
        PathAlgorithm::DijkstraAllPairs
            | PathAlgorithm::FloydWarshall
            | PathAlgorithm::Dfs
            | PathAlgorithm::TransitiveClosure
    ) && target.is_some()
    {
        return Err(GfError::Validation(format!(
            "{by} does not accept a target selector"
        )));
    }
    if matches!(
        by,
        PathAlgorithm::AStar
            | PathAlgorithm::Yens
            | PathAlgorithm::MaxFlow
            | PathAlgorithm::MaxFlowEdges
            | PathAlgorithm::MinCut
            | PathAlgorithm::MinCutEdges
            | PathAlgorithm::MinCostMaxFlow
            | PathAlgorithm::MinCostMaxFlowEdges
    ) && target.is_none()
    {
        return Err(GfError::Validation(format!(
            "{by} requires a target selector"
        )));
    }
    if matches!(by, PathAlgorithm::Yens) && k == 0 {
        return Err(GfError::Validation("yens k must be at least 1".into()));
    }
    if matches!(by, PathAlgorithm::RandomWalk) {
        normalize_random_walk_options(target, k, options.walk_length, options.seed)?;
    } else if options.walk_length.is_some() || options.seed.is_some() {
        return Err(GfError::Validation(format!(
            "{by} does not accept random-walk options"
        )));
    }
    if matches!(
        by,
        PathAlgorithm::Bfs | PathAlgorithm::Dfs | PathAlgorithm::TransitiveClosure
    ) && weight.is_some()
    {
        return Err(GfError::Validation(format!(
            "{by} does not accept an edge weight property"
        )));
    }
    if let Some(weight) = weight
        && invalid_selector(weight)
    {
        return Err(GfError::Validation(format!(
            "invalid paths weight property {weight:?}"
        )));
    }
    validate_min_cost_properties(by, min_cost, weight, capacity_property, cost_property)?;
    validate_heuristic(by, heuristic)?;
    Ok(())
}

fn validate_gomory_hu_invocation(
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
) -> Result<(), GfError> {
    if !matches!(options.by, PathAlgorithm::GomoryHuTree) {
        return Ok(());
    }
    if source.is_some() || target.is_some() {
        return Err(GfError::Validation(format!(
            "{} does not accept positional source or target selectors",
            options.by
        )));
    }
    if options.directed {
        return Err(GfError::Validation(
            "gomory_hu_tree requires directed=false".into(),
        ));
    }
    Ok(())
}

fn validate_source_and_steiner_fields(
    source: Option<[u8; 16]>,
    options: &PathsOptions,
) -> Result<(), GfError> {
    let by = options.by;
    let steiner = steiner_kind(by).is_some();
    let source_free = steiner || matches!(by, PathAlgorithm::GomoryHuTree);
    if !source_free && source.is_none() {
        return Err(GfError::Validation(format!(
            "{by} requires a source selector"
        )));
    }
    if !steiner && !options.terminal_uuids.is_empty() {
        return Err(GfError::Validation(format!(
            "{by} does not accept terminal UUIDs"
        )));
    }
    if !steiner && options.prize_property.is_some() {
        return Err(GfError::Validation(format!(
            "{by} does not accept a prize property"
        )));
    }
    Ok(())
}

const fn steiner_kind(by: PathAlgorithm) -> Option<SteinerKind> {
    match by {
        PathAlgorithm::MinSteinerTree => Some(SteinerKind::MinimumTree),
        PathAlgorithm::PrizeCollectingSteinerTree => Some(SteinerKind::PrizeCollecting),
        _ => None,
    }
}

fn validate_heuristic(by: PathAlgorithm, heuristic: Option<&str>) -> Result<(), GfError> {
    if let Some(heuristic) = heuristic
        && invalid_selector(heuristic)
    {
        return Err(GfError::Validation(format!(
            "invalid paths heuristic property {heuristic:?}"
        )));
    }
    if !matches!(by, PathAlgorithm::AStar) && heuristic.is_some() {
        return Err(GfError::Validation(format!(
            "{by} does not accept a heuristic property"
        )));
    }
    Ok(())
}

fn validate_min_cost_properties(
    by: PathAlgorithm,
    min_cost: bool,
    weight: Option<&str>,
    capacity_property: Option<&str>,
    cost_property: Option<&str>,
) -> Result<(), GfError> {
    if min_cost && weight.is_some() {
        return Err(GfError::Validation(format!(
            "{by} uses capacity_property and cost_property instead of weight"
        )));
    }
    if !min_cost && (capacity_property.is_some() || cost_property.is_some()) {
        return Err(GfError::Validation(format!(
            "{by} does not accept min-cost flow properties"
        )));
    }
    if min_cost && cost_property.is_none() {
        return Err(GfError::Validation(format!(
            "{by} requires a cost_property"
        )));
    }
    for (name, property) in [("capacity", capacity_property), ("cost", cost_property)] {
        if let Some(property) = property
            && invalid_selector(property)
        {
            return Err(GfError::Validation(format!(
                "invalid paths {name} property {property:?}"
            )));
        }
    }
    Ok(())
}

fn invalid_selector(value: &str) -> bool {
    value.is_empty() || value.trim() != value || value.chars().any(char::is_control)
}

const RANDOM_WALK_DEFAULT_LENGTH: usize = 10;
const RANDOM_WALK_DEFAULT_SEED: u64 = 0;

fn normalize_random_walk_options(
    target: Option<[u8; 16]>,
    k: usize,
    walk_length: Option<usize>,
    seed: Option<u64>,
) -> Result<(usize, usize, u64), GfError> {
    if target.is_some() {
        return Err(GfError::Validation(
            "random_walk does not accept a target selector".into(),
        ));
    }
    if k == 0 {
        return Err(GfError::Validation(
            "random_walk k must be at least 1".into(),
        ));
    }
    Ok((
        k,
        walk_length.unwrap_or(RANDOM_WALK_DEFAULT_LENGTH),
        seed.unwrap_or(RANDOM_WALK_DEFAULT_SEED),
    ))
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(id: u64) -> [u8; 16] {
        u128::from(id).to_be_bytes()
    }

    fn value(id: u64) -> AlgorithmValue {
        AlgorithmValue::Uuid(uuid(id))
    }

    fn path(ids: &[u64]) -> AlgorithmValue {
        AlgorithmValue::UuidList(ids.iter().map(|&id| uuid(id)).collect())
    }

    fn traversal(node: u64, depth: u64, order: u64) -> Vec<AlgorithmValue> {
        vec![
            value(node),
            AlgorithmValue::UInt64(depth),
            AlgorithmValue::UInt64(order),
        ]
    }

    fn execute(
        graph: &AdjacencyGraph,
        source: u64,
        target: Option<u64>,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Paths(PathAlgorithm::Bfs);
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(&mut registry, uuid(source), target.map(uuid), 1, None, None)?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_dfs(
        graph: &AdjacencyGraph,
        source: u64,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Paths(PathAlgorithm::Dfs);
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(&mut registry, uuid(source), None, 1, None, None)?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_path_with_compute_threads(
        graph: &AdjacencyGraph,
        algorithm: PathAlgorithm,
        source: u64,
        target: Option<u64>,
        threads: usize,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(&mut registry, uuid(source), target.map(uuid), 1, None, None)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
        registry.execute(Algorithm::Paths(algorithm), graph, &control)
    }

    fn output_fingerprint(output: &AlgorithmOutput) -> String {
        format!("{:?}|{:?}", output.schema, output.rows())
    }

    fn execute_gomory_hu(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Paths(PathAlgorithm::GomoryHuTree);
        let mut registry = AlgorithmRegistry::default();
        registry.register(Arc::new(GomoryHuTree))?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_yens(
        graph: &AdjacencyGraph,
        source: u64,
        target: u64,
        k: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Paths(PathAlgorithm::Yens);
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(
            &mut registry,
            uuid(source),
            Some(uuid(target)),
            k,
            None,
            None,
        )?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_transitive_closure(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Paths(PathAlgorithm::TransitiveClosure);
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(&mut registry, uuid(0), None, 1, None, None)?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_flow(
        graph: &AdjacencyGraph,
        algorithm: PathAlgorithm,
        source: u64,
        target: u64,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(
            &mut registry,
            uuid(source),
            Some(uuid(target)),
            1,
            None,
            None,
        )?;
        registry.execute(
            Algorithm::Paths(algorithm),
            graph,
            &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
        )
    }

    fn execute_min_steiner(
        graph: &AdjacencyGraph,
        terminals: &[u64],
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        registry.register(Arc::new(MinSteinerTree {
            terminals: terminals.iter().copied().map(uuid).collect(),
        }))?;
        registry.execute(
            Algorithm::Paths(PathAlgorithm::MinSteinerTree),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_prize_steiner(
        graph: &AdjacencyGraph,
        terminals: &[u64],
        prizes: &[(u64, f64)],
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        registry.register(Arc::new(PrizeCollectingSteinerTree {
            terminals: terminals.iter().copied().map(uuid).collect(),
            prizes: prizes
                .iter()
                .map(|(node, prize)| NodePrize {
                    node_uuid: uuid(*node),
                    prize: ResolvedNumber::Float64(*prize),
                })
                .collect(),
        }))?;
        registry.execute(
            Algorithm::Paths(PathAlgorithm::PrizeCollectingSteinerTree),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_cut(
        graph: &AdjacencyGraph,
        algorithm: PathAlgorithm,
        source: u64,
        target: u64,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(
            &mut registry,
            uuid(source),
            Some(uuid(target)),
            1,
            None,
            None,
        )?;
        registry.execute(
            Algorithm::Paths(algorithm),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_min_cost_flow(
        graph: &AdjacencyGraph,
        algorithm: PathAlgorithm,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let input: Arc<[CostCapacityEdge]> = Arc::from([
            CostCapacityEdge {
                edge_uuid: uuid(10),
                source_uuid: uuid(0),
                target_uuid: uuid(1),
                capacity: 2.0,
                unit_cost: -1.0,
            },
            CostCapacityEdge {
                edge_uuid: uuid(11),
                source_uuid: uuid(1),
                target_uuid: uuid(2),
                capacity: 2.0,
                unit_cost: 3.0,
            },
            CostCapacityEdge {
                edge_uuid: uuid(12),
                source_uuid: uuid(0),
                target_uuid: uuid(2),
                capacity: 1.0,
                unit_cost: 5.0,
            },
        ]);
        let mut registry = AlgorithmRegistry::default();
        register_path_algorithms(&mut registry, uuid(0), Some(uuid(2)), 1, None, Some(input))?;
        registry.execute(
            Algorithm::Paths(algorithm),
            graph,
            &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
        )
    }

    fn execute_random_walk(
        graph: &AdjacencyGraph,
        source: u64,
        k: usize,
        walk_length: usize,
        seed: u64,
        weighted: bool,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        RandomWalk {
            source: uuid(source),
            k,
            walk_length,
            seed,
            weighted,
        }
        .execute(
            graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        )
    }

    #[test]
    fn gomory_hu_dispatch_shapes_canonical_forest_and_honors_controls() {
        for graph in [
            AdjacencyGraph::with_test_counts(0, 0),
            AdjacencyGraph::with_test_counts(1, 0),
        ] {
            assert!(
                execute_gomory_hu(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
                .rows()
                .is_empty()
            );
        }
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 0, 1), (11, 0, 2), (12, 1, 2)],
        )
        .with_test_edge_weights(&[3.0, 3.0, 2.0, 2.0, 4.0, 4.0]);
        let output = execute_gomory_hu(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Paths(PathAlgorithm::GomoryHuTree).result_schema()
        );
        assert_eq!(
            output.rows(),
            vec![
                vec![value(0), value(1), AlgorithmValue::Float64(5.0),],
                vec![value(1), value(2), AlgorithmValue::Float64(6.0),],
            ]
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_gomory_hu(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_gomory_hu(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn random_walk_dispatch_preserves_seeded_rows_direction_and_weights() {
        let directed = AdjacencyGraph::with_test_directed_edges(4, &[(0, 2), (0, 1), (1, 3)]);
        assert_eq!(
            execute_random_walk(&directed, 0, 2, 3, 42, false)
                .unwrap()
                .rows(),
            vec![
                vec![value(0), path(&[0, 2])],
                vec![value(0), path(&[0, 1, 3])],
            ]
        );
        assert_eq!(
            execute_random_walk(&directed, 2, 1, 3, 42, false)
                .unwrap()
                .rows(),
            vec![vec![value(2), path(&[2])]]
        );

        let undirected = AdjacencyGraph::with_test_undirected_multigraph(2, &[(9, 0, 1)]);
        assert_eq!(
            execute_random_walk(&undirected, 1, 1, 1, 0, false)
                .unwrap()
                .rows(),
            vec![vec![value(1), path(&[1, 0])]]
        );

        let weighted = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (0, 2)])
            .with_test_edge_weights(&[0.0, 1.0]);
        assert_eq!(
            execute_random_walk(&weighted, 0, 1, 1, 0, true)
                .unwrap()
                .rows(),
            vec![vec![value(0), path(&[0, 2])]]
        );
    }

    #[test]
    fn min_cost_flow_views_share_one_typed_solution_and_limits() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (0, 2)]);
        let scalar = execute_min_cost_flow(
            &graph,
            PathAlgorithm::MinCostMaxFlow,
            AlgorithmLimits::default(),
        )
        .unwrap();
        assert_eq!(
            scalar.rows(),
            vec![vec![
                value(0),
                value(2),
                AlgorithmValue::Float64(3.0),
                AlgorithmValue::Float64(9.0),
            ]]
        );
        assert!(
            execute_min_cost_flow(
                &graph,
                PathAlgorithm::MinCostMaxFlow,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
            )
            .is_ok()
        );
        let edges = execute_min_cost_flow(
            &graph,
            PathAlgorithm::MinCostMaxFlowEdges,
            AlgorithmLimits::default(),
        )
        .unwrap();
        assert_eq!(edges.rows().len(), 3);
        assert_eq!(
            edges.schema,
            Algorithm::Paths(PathAlgorithm::MinCostMaxFlowEdges).result_schema()
        );
        assert!(matches!(
            execute_min_cost_flow(
                &graph,
                PathAlgorithm::MinCostMaxFlowEdges,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert_eq!(
            execute_min_cost_flow(
                &graph,
                PathAlgorithm::MinCostMaxFlow,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
            ),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            })
        );
    }

    #[test]
    fn min_cost_flow_checks_cancellation_before_node_projection_allocation() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        MIN_COST_NODE_PROJECTION_ATTEMPTS.with(|attempts| attempts.set(0));
        let handler = MinCostFlow {
            source: uuid(0),
            target: Some(uuid(2)),
            input: Arc::from([CostCapacityEdge {
                edge_uuid: uuid(10),
                source_uuid: uuid(0),
                target_uuid: uuid(1),
                capacity: 1.0,
                unit_cost: 0.0,
            }]),
            edges: false,
        };

        assert_eq!(
            handler.execute(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation,),
            ),
            Err(AlgorithmError::Cancelled)
        );
        MIN_COST_NODE_PROJECTION_ATTEMPTS.with(|attempts| {
            assert_eq!(
                attempts.get(),
                0,
                "cancelled execution reached node projection"
            );
        });
    }

    #[test]
    fn random_walk_dispatch_shapes_canonical_arrow_uuid_lists() {
        use arrow::array::{FixedSizeBinaryArray, ListArray};

        let output = execute_random_walk(
            &AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]),
            0,
            1,
            3,
            7,
            false,
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Paths(PathAlgorithm::RandomWalk).result_schema()
        );
        let batch =
            shape_algorithm_output(Algorithm::Paths(PathAlgorithm::RandomWalk), &output).unwrap();
        assert_eq!(batch.schema().fields()[0].name(), "start_uuid");
        assert_eq!(batch.schema().fields()[1].name(), "walk");
        let walks = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = walks.value(0);
        let values = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), uuid(0));
        assert_eq!(values.value(1), uuid(1));
    }

    #[test]
    fn maximum_flow_views_share_one_canonical_solution_and_apply_view_limits() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
                .with_test_edge_weights(&[3.0, 2.0, 1.0, 2.0, 4.0]);
        let scalar = execute_flow(
            &graph,
            PathAlgorithm::MaxFlow,
            0,
            3,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
        )
        .unwrap();
        let edges = execute_flow(
            &graph,
            PathAlgorithm::MaxFlowEdges,
            0,
            3,
            AlgorithmLimits::default(),
        )
        .unwrap();
        assert_eq!(
            scalar,
            crate::algorithm_output::shape_logical_rows(
                Algorithm::Paths(PathAlgorithm::MaxFlow),
                vec![vec![value(0), value(3), AlgorithmValue::Float64(5.0)]],
                8192,
                u64::MAX
            )
            .unwrap()
        );
        assert_eq!(
            edges.rows().iter().map(|row| &row[3]).collect::<Vec<_>>(),
            vec![
                &AlgorithmValue::Float64(3.0),
                &AlgorithmValue::Float64(2.0),
                &AlgorithmValue::Float64(1.0),
                &AlgorithmValue::Float64(2.0),
                &AlgorithmValue::Float64(3.0),
            ]
        );
        assert!(matches!(
            execute_flow(
                &graph,
                PathAlgorithm::MaxFlowEdges,
                0,
                3,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
            ),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1
            })
        ));
    }

    #[test]
    fn minimum_cut_views_shape_one_shared_canonical_solution() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
                .with_test_edge_weights(&[3.0, 2.0, 1.0, 2.0, 4.0]);
        let scalar = execute_cut(
            &graph,
            PathAlgorithm::MinCut,
            0,
            3,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let edges = execute_cut(
            &graph,
            PathAlgorithm::MinCutEdges,
            0,
            3,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            scalar,
            crate::algorithm_output::shape_logical_rows(
                Algorithm::Paths(PathAlgorithm::MinCut),
                vec![vec![value(0), value(3), AlgorithmValue::Float64(5.0)]],
                8192,
                u64::MAX
            )
            .unwrap()
        );
        assert_eq!(
            edges,
            crate::algorithm_output::shape_logical_rows(
                Algorithm::Paths(PathAlgorithm::MinCutEdges),
                vec![
                    vec![value(0), value(0), value(1), AlgorithmValue::Float64(3.0)],
                    vec![value(1), value(0), value(2), AlgorithmValue::Float64(2.0)],
                ],
                8192,
                u64::MAX
            )
            .unwrap()
        );
        assert_eq!(
            scalar.rows()[0][2],
            AlgorithmValue::Float64(
                edges
                    .rows()
                    .iter()
                    .map(|row| match &row[3] {
                        AlgorithmValue::Float64(capacity) => *capacity,
                        _ => unreachable!("minimum-cut edge capacity is Float64"),
                    })
                    .sum()
            )
        );
        for (algorithm, output, fields) in [
            (
                PathAlgorithm::MinCut,
                &scalar,
                vec!["source_uuid", "sink_uuid", "cut_value"],
            ),
            (
                PathAlgorithm::MinCutEdges,
                &edges,
                vec!["edge_uuid", "source_uuid", "target_uuid", "capacity"],
            ),
        ] {
            let batch = shape_algorithm_output(Algorithm::Paths(algorithm), output).unwrap();
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                fields
            );
            assert!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| !field.is_nullable())
            );
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm"],
                algorithm.as_str()
            );
            assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
        }
    }

    #[test]
    fn minimum_cut_edges_preserve_undirected_storage_orientation_and_zero_results() {
        let undirected = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 0, 1), (11, 1, 2), (12, 2, 3)],
        )
        .with_test_edge_weights(&[2.0; 6]);
        assert_eq!(
            execute_cut(
                &undirected,
                PathAlgorithm::MinCutEdges,
                3,
                0,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            vec![vec![
                value(10),
                value(0),
                value(1),
                AlgorithmValue::Float64(2.0),
            ]]
        );

        let unreachable = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1)]);
        assert_eq!(
            execute_cut(
                &unreachable,
                PathAlgorithm::MinCut,
                0,
                2,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            vec![vec![value(0), value(2), AlgorithmValue::Float64(0.0)]]
        );
        assert!(
            execute_cut(
                &unreachable,
                PathAlgorithm::MinCutEdges,
                0,
                2,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn minimum_cut_dispatch_rejects_invalid_inputs_and_propagates_controls() {
        let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute_cut(
                &graph,
                PathAlgorithm::MinCut,
                0,
                0,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "minimum cut requires distinct endpoints"
        ));
        let invalid = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)])
            .with_test_edge_weights(&[f64::NAN]);
        assert!(matches!(
            execute_cut(
                &invalid,
                PathAlgorithm::MinCut,
                0,
                1,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "minimum cut requires finite nonnegative capacities"
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            execute_cut(
                &graph,
                PathAlgorithm::MinCut,
                0,
                1,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        ));
        assert!(matches!(
            execute_cut(
                &graph,
                PathAlgorithm::MinCutEdges,
                0,
                1,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit {
                observed: 1,
                limit: 0
            })
        ));
    }

    #[test]
    fn undirected_flow_rows_use_canonical_endpoints_and_signed_assignments() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 1, 0), (11, 2, 1), (12, 3, 2)],
        )
        .with_test_edge_weights(&[2.0, 2.0, 2.0, 2.0, 2.0, 2.0]);
        let rows: Vec<Vec<AlgorithmValue>> = execute_flow(
            &graph,
            PathAlgorithm::MaxFlowEdges,
            3,
            0,
            AlgorithmLimits::default(),
        )
        .unwrap()
        .rows();
        assert_eq!(
            rows,
            vec![
                vec![value(10), value(0), value(1), AlgorithmValue::Float64(-2.0)],
                vec![value(11), value(1), value(2), AlgorithmValue::Float64(-2.0)],
                vec![value(12), value(2), value(3), AlgorithmValue::Float64(-2.0)],
            ]
        );
    }

    #[test]
    fn transitive_closure_dispatch_shapes_uuid_pairs_and_propagates_controls() {
        let graph = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 0), (1, 2), (3, 3)]);
        let output = execute_transitive_closure(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Paths(PathAlgorithm::TransitiveClosure).result_schema()
        );
        assert_eq!(
            output.rows(),
            vec![
                vec![value(0), value(0)],
                vec![value(0), value(1)],
                vec![value(0), value(2)],
                vec![value(1), value(0)],
                vec![value(1), value(1)],
                vec![value(1), value(2)],
                vec![value(3), value(3)],
            ]
        );

        assert!(matches!(
            execute_transitive_closure(
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
            execute_transitive_closure(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn yens_dispatch_shapes_ranked_uuid_rows_and_propagates_controls() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 3), (0, 2), (2, 3), (0, 3)])
            .with_test_edge_weights(&[1.0, 2.0, 1.0, 2.0, 4.0]);
        let output = execute_yens(
            &graph,
            0,
            3,
            3,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Paths(PathAlgorithm::Yens).result_schema()
        );
        assert_eq!(
            output.rows(),
            vec![
                vec![
                    value(0),
                    value(3),
                    AlgorithmValue::UInt64(1),
                    AlgorithmValue::Float64(3.0),
                    path(&[0, 1, 3]),
                ],
                vec![
                    value(0),
                    value(3),
                    AlgorithmValue::UInt64(2),
                    AlgorithmValue::Float64(3.0),
                    path(&[0, 2, 3]),
                ],
                vec![
                    value(0),
                    value(3),
                    AlgorithmValue::UInt64(3),
                    AlgorithmValue::Float64(4.0),
                    path(&[0, 3]),
                ],
            ]
        );
        assert!(matches!(
            execute_yens(
                &graph,
                0,
                3,
                3,
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
            execute_yens(&graph, 0, 3, 3, AlgorithmLimits::default(), cancellation,).unwrap_err(),
            AlgorithmError::Cancelled
        );
    }

    #[test]
    fn bfs_returns_deterministic_shortest_paths_for_target_and_all_reachable() {
        let graph = AdjacencyGraph::with_test_edges(6, &[(0, 2), (0, 1), (2, 3), (1, 3), (3, 4)]);
        let all = execute(
            &graph,
            0,
            None,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let schema = Algorithm::Paths(PathAlgorithm::Bfs).result_schema();
        assert_eq!(all.schema, schema);
        assert_eq!(
            all.rows(),
            vec![
                vec![value(0), value(0), AlgorithmValue::Float64(0.0), path(&[0])],
                vec![
                    value(0),
                    value(1),
                    AlgorithmValue::Float64(1.0),
                    path(&[0, 1])
                ],
                vec![
                    value(0),
                    value(2),
                    AlgorithmValue::Float64(1.0),
                    path(&[0, 2])
                ],
                vec![
                    value(0),
                    value(3),
                    AlgorithmValue::Float64(2.0),
                    path(&[0, 1, 3])
                ],
                vec![
                    value(0),
                    value(4),
                    AlgorithmValue::Float64(3.0),
                    path(&[0, 1, 3, 4])
                ],
            ]
        );
        assert_eq!(
            execute(
                &graph,
                0,
                Some(4),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            vec![vec![
                value(0),
                value(4),
                AlgorithmValue::Float64(3.0),
                path(&[0, 1, 3, 4]),
            ]]
        );
    }

    #[test]
    fn bfs_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            8,
            &[
                (0, 2),
                (0, 1),
                (0, 1),
                (1, 3),
                (1, 4),
                (2, 4),
                (2, 5),
                (3, 6),
                (4, 6),
                (5, 7),
                (7, 7),
            ],
        );
        let serial =
            execute_path_with_compute_threads(&graph, PathAlgorithm::Bfs, 0, None, 1).unwrap();
        assert_eq!(
            serial.rows(),
            vec![
                vec![value(0), value(0), AlgorithmValue::Float64(0.0), path(&[0])],
                vec![
                    value(0),
                    value(1),
                    AlgorithmValue::Float64(1.0),
                    path(&[0, 1]),
                ],
                vec![
                    value(0),
                    value(2),
                    AlgorithmValue::Float64(1.0),
                    path(&[0, 2]),
                ],
                vec![
                    value(0),
                    value(3),
                    AlgorithmValue::Float64(2.0),
                    path(&[0, 1, 3]),
                ],
                vec![
                    value(0),
                    value(4),
                    AlgorithmValue::Float64(2.0),
                    path(&[0, 1, 4]),
                ],
                vec![
                    value(0),
                    value(5),
                    AlgorithmValue::Float64(2.0),
                    path(&[0, 2, 5]),
                ],
                vec![
                    value(0),
                    value(6),
                    AlgorithmValue::Float64(3.0),
                    path(&[0, 1, 3, 6]),
                ],
                vec![
                    value(0),
                    value(7),
                    AlgorithmValue::Float64(3.0),
                    path(&[0, 2, 5, 7]),
                ],
            ]
        );
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output =
                execute_path_with_compute_threads(&graph, PathAlgorithm::Bfs, 0, None, threads)
                    .unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }

    #[test]
    fn dfs_dispatch_shapes_traversal_rows_and_preserves_boundaries() {
        let graph = AdjacencyGraph::with_test_directed_edges(5, &[(0, 2), (0, 1), (1, 3), (2, 3)]);
        let output = execute_dfs(
            &graph,
            0,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Paths(PathAlgorithm::Dfs).result_schema()
        );
        assert_eq!(
            output.rows(),
            vec![
                traversal(0, 0, 0),
                traversal(1, 1, 1),
                traversal(3, 2, 2),
                traversal(2, 1, 3),
            ]
        );
        assert!(matches!(
            execute_dfs(
                &graph,
                9,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "dfs source UUID is not in the selected graph"
        ));
        assert!(matches!(
            execute_dfs(
                &graph,
                0,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit {
                observed: 3,
                limit: 2
            })
        ));
        assert!(matches!(
            execute_dfs(
                &graph,
                0,
                AlgorithmLimits {
                    nodes: 4,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit {
                observed: 5,
                limit: 4
            })
        ));
        assert!(matches!(
            execute_dfs(
                &AdjacencyGraph::default(),
                0,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "dfs source UUID is not in the selected graph"
        ));
    }

    #[test]
    fn dfs_rejects_non_single_source_options() {
        for result in [
            validate_path_options(
                Some(uuid(0)),
                None,
                &PathsOptions {
                    by: PathAlgorithm::Dfs,
                    k: 2,
                    ..PathsOptions::default()
                },
            ),
            validate_path_options(
                Some(uuid(0)),
                Some(uuid(1)),
                &PathsOptions {
                    by: PathAlgorithm::Dfs,
                    ..PathsOptions::default()
                },
            ),
            validate_path_options(
                Some(uuid(0)),
                None,
                &PathsOptions {
                    by: PathAlgorithm::Dfs,
                    weight: Some("cost".into()),
                    ..PathsOptions::default()
                },
            ),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn dfs_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            8,
            &[
                (0, 2),
                (0, 1),
                (0, 1),
                (1, 3),
                (1, 4),
                (3, 6),
                (6, 1),
                (2, 5),
                (5, 7),
                (7, 7),
            ],
        );
        let serial =
            execute_path_with_compute_threads(&graph, PathAlgorithm::Dfs, 0, None, 1).unwrap();
        assert_eq!(
            serial.rows(),
            vec![
                traversal(0, 0, 0),
                traversal(1, 1, 1),
                traversal(3, 2, 2),
                traversal(6, 3, 3),
                traversal(4, 2, 4),
                traversal(2, 1, 5),
                traversal(5, 2, 6),
                traversal(7, 3, 7),
            ]
        );
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output =
                execute_path_with_compute_threads(&graph, PathAlgorithm::Dfs, 0, None, threads)
                    .unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }

    #[test]
    fn maximum_flow_views_reject_noncanonical_k() {
        for by in [
            PathAlgorithm::MaxFlow,
            PathAlgorithm::MaxFlowEdges,
            PathAlgorithm::MinCut,
            PathAlgorithm::MinCutEdges,
        ] {
            for k in [0, 2] {
                assert!(matches!(
                    validate_path_options(
                        Some(uuid(0)),
                        Some(uuid(1)),
                        &PathsOptions {
                            by,
                            k,
                            ..PathsOptions::default()
                        },
                    ),
                    Err(GfError::Validation(message))
                        if message == format!("{by} k must be 1")
                ));
            }
        }
    }

    #[test]
    fn minimum_cut_views_require_target_and_reject_unrelated_options() {
        for by in [PathAlgorithm::MinCut, PathAlgorithm::MinCutEdges] {
            assert!(matches!(
                validate_path_options(
                    Some(uuid(0)),
                    None,
                    &PathsOptions {
                        by,
                        ..PathsOptions::default()
                    },
                ),
                Err(GfError::Validation(message))
                    if message == format!("{by} requires a target selector")
            ));
            assert!(matches!(
                validate_path_options(
                    Some(uuid(0)),
                    Some(uuid(1)),
                    &PathsOptions {
                        by,
                        heuristic: Some("estimate".into()),
                        ..PathsOptions::default()
                    },
                ),
                Err(GfError::Validation(message))
                    if message == format!("{by} does not accept a heuristic property")
            ));
            assert!(matches!(
                validate_path_options(
                    Some(uuid(0)),
                    Some(uuid(1)),
                    &PathsOptions {
                        by,
                        seed: Some(7),
                        ..PathsOptions::default()
                    },
                ),
                Err(GfError::Validation(message))
                    if message == format!("{by} does not accept random-walk options")
            ));
        }
    }

    #[test]
    fn min_cost_flow_public_options_require_exact_capacity_and_cost_contract() {
        for by in [
            PathAlgorithm::MinCostMaxFlow,
            PathAlgorithm::MinCostMaxFlowEdges,
        ] {
            let validate = |options: PathsOptions| {
                validate_path_options(Some(uuid(0)), Some(uuid(1)), &options)
            };
            assert!(matches!(
                validate(PathsOptions {
                    by,
                    weight: Some("weight".into()),
                    capacity_property: Some("capacity".into()),
                    cost_property: Some("cost".into()),
                    ..PathsOptions::default()
                }),
                Err(GfError::Validation(message))
                    if message == format!(
                        "{by} uses capacity_property and cost_property instead of weight"
                    )
            ));
            assert!(matches!(
                validate(PathsOptions {
                    by,
                    capacity_property: Some("capacity".into()),
                    ..PathsOptions::default()
                }),
                Err(GfError::Validation(message))
                    if message == format!("{by} requires a cost_property")
            ));
            assert!(matches!(
                validate(PathsOptions {
                    by,
                    capacity_property: Some(" bad".into()),
                    cost_property: Some("cost".into()),
                    ..PathsOptions::default()
                }),
                Err(GfError::Validation(message))
                    if message == "invalid paths capacity property \" bad\""
            ));
            assert!(
                validate(PathsOptions {
                    by,
                    capacity_property: Some("capacity".into()),
                    cost_property: Some("cost".into()),
                    ..PathsOptions::default()
                })
                .is_ok()
            );
        }

        assert!(matches!(
            validate_path_options(
                Some(uuid(0)),
                Some(uuid(1)),
                &PathsOptions {
                    by: PathAlgorithm::MaxFlow,
                    capacity_property: Some("capacity".into()),
                    cost_property: Some("cost".into()),
                    ..PathsOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "max_flow does not accept min-cost flow properties"
        ));
    }

    #[test]
    fn public_projection_fingerprint_is_stable_across_provider_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let options = PathsOptions {
            by: PathAlgorithm::FloydWarshall,
            ..PathsOptions::default()
        };
        let first_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let first = paths_projection_fingerprint(
            &first_provider,
            dir.path(),
            OntologyMode::Strict,
            Some(uuid(0)),
            None,
            &options,
        )
        .unwrap();
        drop(first_provider);
        let reopened_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let reopened = paths_projection_fingerprint(
            &reopened_provider,
            dir.path(),
            OntologyMode::Strict,
            Some(uuid(0)),
            None,
            &options,
        )
        .unwrap();
        assert_eq!(first, reopened);
        assert_ne!(first, [0; 32]);
    }

    #[test]
    fn random_walk_options_normalize_defaults_and_preserve_explicit_controls() {
        assert_eq!(
            normalize_random_walk_options(None, 1, None, None).unwrap(),
            (1, RANDOM_WALK_DEFAULT_LENGTH, RANDOM_WALK_DEFAULT_SEED)
        );
        assert_eq!(
            normalize_random_walk_options(None, 3, Some(0), Some(42)).unwrap(),
            (3, 0, 42)
        );
    }

    #[test]
    fn random_walk_options_reject_zero_count_and_target_selector() {
        assert!(matches!(
            normalize_random_walk_options(None, 0, Some(10), Some(42)),
            Err(GfError::Validation(message))
                if message == "random_walk k must be at least 1"
        ));
        assert!(matches!(
            normalize_random_walk_options(Some(uuid(1)), 1, Some(10), Some(42)),
            Err(GfError::Validation(message))
                if message == "random_walk does not accept a target selector"
        ));
    }

    #[test]
    fn non_random_walk_catalogs_reject_random_walk_options() {
        assert!(matches!(
            validate_path_options(
                Some(uuid(0)),
                None,
                &PathsOptions {
                    by: PathAlgorithm::Bfs,
                    walk_length: Some(10),
                    seed: Some(42),
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == "bfs does not accept random-walk options"
        ));
    }

    #[test]
    fn source_and_steiner_fields_follow_closed_catalog_policy() {
        for by in [PathAlgorithm::Bfs, PathAlgorithm::RandomWalk] {
            assert!(matches!(
                validate_path_options(
                    None,
                    None,
                    &PathsOptions {
                        by,
                        ..PathsOptions::default()
                    },
                ),
                Err(GfError::Validation(message))
                    if message == format!("{by} requires a source selector")
            ));
        }
        for options in [
            PathsOptions {
                by: PathAlgorithm::Bfs,
                terminal_uuids: vec![uuid(1)],
                ..PathsOptions::default()
            },
            PathsOptions {
                by: PathAlgorithm::Bfs,
                prize_property: Some("prize".into()),
                ..PathsOptions::default()
            },
        ] {
            assert!(matches!(
                validate_path_options(Some(uuid(0)), None, &options),
                Err(GfError::Validation(_))
            ));
        }
        for by in [
            PathAlgorithm::MinSteinerTree,
            PathAlgorithm::PrizeCollectingSteinerTree,
        ] {
            assert!(
                validate_path_options(
                    None,
                    None,
                    &PathsOptions {
                        by,
                        ..PathsOptions::default()
                    },
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn gomory_hu_public_validation_rejects_positional_and_directed_requests() {
        let positional = PathsOptions {
            by: PathAlgorithm::GomoryHuTree,
            ..PathsOptions::default()
        };
        assert!(matches!(
            validate_path_options(Some(uuid(0)), None, &positional),
            Err(GfError::Validation(message))
                if message.contains("does not accept positional source or target")
        ));

        let directed = PathsOptions {
            by: PathAlgorithm::GomoryHuTree,
            directed: true,
            ..PathsOptions::default()
        };
        assert!(matches!(
            validate_path_options(None, None, &directed),
            Err(GfError::Validation(message)) if message == "gomory_hu_tree requires directed=false"
        ));
    }

    #[test]
    fn minimum_steiner_dispatch_preserves_atomic_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(9, 0, 1), (8, 1, 2), (7, 2, 3), (6, 0, 3)],
        );
        let output = execute_min_steiner(
            &graph,
            &[0, 2],
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(output.rows().len(), 2);
        assert_eq!(
            output.rows().iter().map(|row| &row[0]).collect::<Vec<_>>(),
            [&value(6), &value(7)]
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            execute_min_steiner(&graph, &[0, 2], AlgorithmLimits::default(), cancellation,),
            Err(AlgorithmError::Cancelled)
        ));
        for limits in [
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                states: 0,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(
                execute_min_steiner(&graph, &[0, 2], limits, AlgorithmCancellation::default(),)
                    .is_err()
            );
        }
        assert_eq!(
            execute_min_steiner(
                &graph,
                &[0, 2],
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            output.rows()
        );
    }

    #[test]
    fn prize_steiner_dispatch_preserves_atomic_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            3,
            &[(9, 0, 1), (8, 0, 1), (7, 0, 2), (6, 1, 1)],
        );
        let prizes = [(0, 0.0), (1, 3.0), (2, 0.0)];
        let output = execute_prize_steiner(
            &graph,
            &[0],
            &prizes,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.rows(),
            vec![vec![
                value(8),
                value(0),
                value(1),
                AlgorithmValue::Float64(1.0)
            ]]
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            execute_prize_steiner(
                &graph,
                &[0],
                &prizes,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        ));
        for limits in [
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                states: 0,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(
                execute_prize_steiner(
                    &graph,
                    &[0],
                    &prizes,
                    limits,
                    AlgorithmCancellation::default(),
                )
                .is_err()
            );
        }
        assert_eq!(
            execute_prize_steiner(
                &graph,
                &[0],
                &prizes,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            output.rows()
        );
    }

    #[test]
    fn bfs_handles_multigraph_boundaries_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 0), (0, 1), (0, 1), (1, 2), (2, 1)]);
        assert_eq!(
            execute(
                &graph,
                0,
                Some(3),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            Vec::<Vec<AlgorithmValue>>::new()
        );
        assert!(matches!(
            execute(
                &graph,
                9,
                None,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            execute(&graph, 0, None, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        ));
        assert!(matches!(
            execute(
                &graph,
                0,
                None,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn numeric_projection_hashes_in_node_order_and_rejects_invalid_values() {
        use sha2::Digest;

        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let values = HashMap::from([(0, 1.25), (1, -2.5), (2, 3.75)]);
        let mut first = Sha256::new();
        update_node_numeric_projection(&mut first, &graph, "heuristic", "distance", &values)
            .expect("finite complete numeric projection");
        let first: [u8; 32] = first.finalize().into();

        let mut reordered = Sha256::new();
        let reordered_values = HashMap::from([(2, 3.75), (0, 1.25), (1, -2.5)]);
        update_node_numeric_projection(
            &mut reordered,
            &graph,
            "heuristic",
            "distance",
            &reordered_values,
        )
        .expect("map insertion order does not affect projection");
        assert_eq!(first, <[u8; 32]>::from(reordered.finalize()));

        let mut missing = Sha256::new();
        let error = update_node_numeric_projection(
            &mut missing,
            &graph,
            "prize",
            "value",
            &HashMap::from([(0, 1.0), (1, 2.0)]),
        )
        .expect_err("every projected node needs a value");
        assert_eq!(
            error.to_string(),
            "execution error: numeric projection node has no property value"
        );

        let mut non_finite = Sha256::new();
        let error = update_node_numeric_projection(
            &mut non_finite,
            &graph,
            "prize",
            "value",
            &HashMap::from([(0, 1.0), (1, f64::NAN), (2, 3.0)]),
        )
        .expect_err("non-finite graph-native values are not fingerprintable");
        assert_eq!(
            error.to_string(),
            "execution error: numeric projection contains a non-finite value"
        );
    }
}
