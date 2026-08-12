//! Rust-owned graph analysis handlers registered under the shared algorithm dispatch contract.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, AnalyzeAlgorithm};
use graphforge_core::embedding_options::{EmbeddingAnalyzeOptions, EmbeddingOptions};
use graphforge_core::{AnalyzeOptions, GfError, OntologyMode, TypeId};
use graphforge_ir::{Direction, IrLiteral};
use sha2::{Digest, Sha256};

use crate::AdjacencyProvider;
use crate::algorithm_analyze_automorphism::{AutomorphismEdge, AutomorphismGraph};
use crate::algorithm_analyze_automorphism_count::count_automorphisms;
use crate::algorithm_analyze_bipartite::{BipartiteEdge, resolve_bipartite_projection};
use crate::algorithm_analyze_bipartite_matching::maximum_bipartite_matching;
use crate::algorithm_analyze_chromatic_number::{ChromaticEdge, exact_chromatic_number};
use crate::algorithm_analyze_conductance::{ConductanceEdge, conductance};
use crate::algorithm_analyze_dag_longest_path::{DagLongestPathEdge, dag_longest_path};
use crate::algorithm_analyze_dag_longest_path_weighted::{
    WeightedDagEdge, weighted_dag_longest_path,
};
use crate::algorithm_analyze_dag_topology::stable_dag_topology;
use crate::algorithm_analyze_dyad_census::{DyadEdge, dyad_census};
use crate::algorithm_analyze_edge_coloring::{EdgeColoringEdge, greedy_edge_coloring};
use crate::algorithm_analyze_euler::{
    EulerEdge, EulerProjection, EulerTrailKind, EulerTrailOutcome,
};
use crate::algorithm_analyze_find_cycles::{CycleEdge, find_cycles};
use crate::algorithm_analyze_has_euler_circuit::{EulerCircuitEdge, has_euler_circuit};
use crate::algorithm_analyze_has_euler_path::{EulerPathEdge, has_euler_path};
use crate::algorithm_analyze_is_planar::{PlanarityEdge, is_planar};
use crate::algorithm_analyze_k1_coloring::k1_coloring;
use crate::algorithm_analyze_lowlink::low_link;
use crate::algorithm_analyze_max_cardinality_matching::{
    MatchingEdge, maximum_cardinality_matching,
};
use crate::algorithm_analyze_minimum_k_spanning_tree::minimum_k_spanning_trees;
use crate::algorithm_analyze_minimum_spanning_forest::{SpanningEdge, spanning_forest};
use crate::algorithm_analyze_modularity::{ModularityEdge, modularity, modularity_output};
use crate::algorithm_analyze_node_coloring::{NodeColoringEdge, greedy_node_coloring};
use crate::algorithm_analyze_transitivity::{TransitivityEdge, transitivity};
use crate::algorithm_analyze_triad_census::{TRIAD_NAMES, TriadEdge, triad_census};
use crate::algorithm_analyze_triangle_count::{TriangleEdge, triangle_count};
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_embedding_control::{
    EmbeddingControl, EmbeddingResourceEstimate, EmbeddingResourceLimits, FastRpResources,
    HashGnnResources, Node2VecResources, TopologyResources,
};
use crate::algorithm_embedding_fastrp::train_fastrp;
use crate::algorithm_embedding_graphsage::{
    GraphSageEdge, GraphSageNode, GraphSageProjection, preflight_graphsage_dispatch,
    train_graphsage, validate_graphsage_projection,
};
use crate::algorithm_embedding_hashgnn::{HashGnnTypeTokens, hashgnn_embeddings};
use crate::algorithm_embedding_invocation::{
    EmbeddingExecution, EmbeddingInvocationDescriptor, EmbeddingInvocationLimits,
    EmbeddingProjectionSelector, EmbeddingRngContract,
};
use crate::algorithm_embedding_node2vec::train_node2vec;
use crate::algorithm_embedding_options::{NormalizedEmbeddingOptions, normalize_embedding_options};
use crate::algorithm_embedding_output::{
    RNG_DERIVATION, RNG_VERSION, SCHEMA_VERSION, shape_embedding_output,
};
use crate::algorithm_graph::{
    AdjacencyGraph, AdjacencySelection, export_adjacency, load_node_feature_properties,
    load_node_partition_property, load_node_scalar_features,
};
use crate::algorithm_output::shape_algorithm_output;
use crate::algorithm_partition::ResolvedPartitionMap;
use crate::algorithm_weighted_undirected::{
    WeightedEdge, normalize_weighted_undirected, solve_exact_matching,
};

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "algorithm-specific canonical UUID and topology ordering",
    platforms: "Rust workspace targets",
};

struct IsDag {
    directed: bool,
}

struct SpanningTree {
    algorithm: AnalyzeAlgorithm,
    maximize: bool,
}
struct MinimumKSpanningTree {
    k: usize,
}

struct TopologicalSort;
struct ArticulationPoints;
struct Bridges;
struct K1Coloring;
struct NodeColoring;
struct ChromaticNumber;
struct TriangleCount;
struct Transitivity;
struct TriadCensus;
struct DyadCensus;
struct DagLongestPath;
struct WeightedDagLongestPath;
struct EdgeColoring;
struct EulerConstruction {
    algorithm: AnalyzeAlgorithm,
    directed: bool,
}
struct FindCycles {
    directed: bool,
}
struct HasEulerCircuit {
    directed: bool,
}
struct HasEulerPath {
    directed: bool,
}
struct IsPlanar;
struct Conductance {
    partitions: ResolvedPartitionMap,
}
struct Modularity {
    partitions: ResolvedPartitionMap,
}
struct MaxBipartiteMatching {
    partitions: Option<ResolvedPartitionMap>,
}
struct MaxCardinalityMatching;
struct MaxWeightMatching;
struct CountAutomorphisms {
    directed: bool,
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

fn automorphism_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "automorphism node has no UUID identity".into(),
        })
}

fn automorphism_allocation(context: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: format!("automorphism {context} allocation failed"),
    }
}

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

fn euler_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
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

fn directed_is_dag(
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

pub(crate) fn register_analyze_algorithms(
    registry: &mut AlgorithmRegistry,
    directed: bool,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(ArticulationPoints))?;
    registry.register(Arc::new(Bridges))?;
    registry.register(Arc::new(ChromaticNumber))?;
    registry.register(Arc::new(CountAutomorphisms { directed }))?;
    registry.register(Arc::new(DagLongestPath))?;
    registry.register(Arc::new(DyadCensus))?;
    registry.register(Arc::new(WeightedDagLongestPath))?;
    registry.register(Arc::new(EdgeColoring))?;
    registry.register(Arc::new(EulerConstruction {
        algorithm: AnalyzeAlgorithm::EulerCircuit,
        directed,
    }))?;
    registry.register(Arc::new(EulerConstruction {
        algorithm: AnalyzeAlgorithm::EulerPath,
        directed,
    }))?;
    registry.register(Arc::new(FindCycles { directed }))?;
    registry.register(Arc::new(HasEulerCircuit { directed }))?;
    registry.register(Arc::new(HasEulerPath { directed }))?;
    registry.register(Arc::new(IsDag { directed }))?;
    registry.register(Arc::new(IsPlanar))?;
    registry.register(Arc::new(K1Coloring))?;
    registry.register(Arc::new(MaxCardinalityMatching))?;
    registry.register(Arc::new(MaxWeightMatching))?;
    registry.register(Arc::new(NodeColoring))?;
    registry.register(Arc::new(SpanningTree {
        algorithm: AnalyzeAlgorithm::MinimumSpanningTree,
        maximize: false,
    }))?;
    registry.register(Arc::new(SpanningTree {
        algorithm: AnalyzeAlgorithm::MaximumSpanningTree,
        maximize: true,
    }))?;
    registry.register(Arc::new(TriangleCount))?;
    registry.register(Arc::new(Transitivity))?;
    registry.register(Arc::new(TriadCensus))?;
    registry.register(Arc::new(TopologicalSort))
}

fn normalize_analyze_options(options: &AnalyzeOptions) -> Result<AnalyzeOptions, GfError> {
    let mut normalized = options.clone();
    if options.by == AnalyzeAlgorithm::MinimumKSpanningTree {
        let k = options.k.unwrap_or(1);
        if k == 0 {
            return Err(GfError::Validation(
                "minimum_k_spanning_tree requires k greater than zero".into(),
            ));
        }
        normalized.k = Some(k);
    } else if options.k.is_some() {
        return Err(GfError::Validation(format!(
            "{} does not accept k",
            options.by
        )));
    }
    match options.by {
        AnalyzeAlgorithm::MaxBipartiteMatching => {
            if options
                .partition_property
                .as_deref()
                .is_some_and(str::is_empty)
            {
                return Err(GfError::Validation(
                    "max_bipartite_matching requires a non-empty partition_property when supplied"
                        .into(),
                ));
            }
        }
        AnalyzeAlgorithm::Conductance => {
            if options
                .partition_property
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(GfError::Validation(
                    "conductance requires a non-empty partition_property".into(),
                ));
            }
        }
        AnalyzeAlgorithm::Modularity => {
            if options
                .partition_property
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(GfError::Validation(
                    "modularity requires a non-empty partition_property".into(),
                ));
            }
        }
        _ if options.partition_property.is_some() => {
            return Err(GfError::Validation(format!(
                "{} does not accept partition_property",
                options.by
            )));
        }
        _ => {}
    }
    Ok(normalized)
}

fn register_option_analyze_algorithm(
    registry: &mut AlgorithmRegistry,
    options: &AnalyzeOptions,
    partitions: Option<ResolvedPartitionMap>,
) -> Result<(), GfError> {
    match options.by {
        AnalyzeAlgorithm::MinimumKSpanningTree => {
            registry.register(Arc::new(MinimumKSpanningTree {
                k: options
                    .k
                    .expect("minimum-k option normalization supplies a positive k"),
            }))?;
        }
        AnalyzeAlgorithm::Conductance => {
            registry.register(Arc::new(Conductance {
                partitions: partitions.expect("conductance projection resolves partitions"),
            }))?;
        }
        AnalyzeAlgorithm::Modularity => {
            registry.register(Arc::new(Modularity {
                partitions: partitions.expect("modularity projection resolves partitions"),
            }))?;
        }
        AnalyzeAlgorithm::MaxBipartiteMatching => {
            registry.register(Arc::new(MaxBipartiteMatching { partitions }))?;
        }
        _ => {}
    }
    Ok(())
}

/// Execute a typed graph analysis algorithm through Rust dispatch and return
/// its canonical Arrow batch.
///
/// # Errors
/// Returns structured validation/execution errors for malformed selection,
/// unavailable algorithms, adjacency reads, limits, or result shaping.
pub fn analyze_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    options: &AnalyzeOptions,
) -> Result<RecordBatch, GfError> {
    let prepared = prepare_analyze_projection(provider, dir, mode, label, options)?;
    let algorithm = Algorithm::Analyze(prepared.options.by);
    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, prepared.options.directed)?;
    register_option_analyze_algorithm(&mut registry, &prepared.options, prepared.partitions)?;
    let output = registry.execute(
        algorithm,
        &prepared.graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )?;
    shape_algorithm_output(algorithm, &output).map_err(Into::into)
}

/// Fingerprint the exact topology, weights, and partition values consumed by analysis.
///
/// # Errors
/// Returns the same projection and option failures as [`analyze_algorithm`].
pub fn analyze_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    options: &AnalyzeOptions,
) -> Result<[u8; 32], GfError> {
    let prepared = prepare_analyze_projection(provider, dir, mode, label, options)?;
    let base = prepared.graph.descriptor_projection_fingerprint()?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge_analyze_projection_v1");
    digest.update(base.as_bytes());
    if let Some(property) = prepared.options.partition_property.as_deref() {
        digest.update(
            u64::try_from(property.len())
                .map_err(|_| GfError::Execution("partition property name is too long".into()))?
                .to_be_bytes(),
        );
        digest.update(property.as_bytes());
    } else {
        digest.update(0_u64.to_be_bytes());
    }
    if let Some(partitions) = prepared.partitions {
        for (uuid, partition) in partitions.iter() {
            digest.update(uuid);
            digest.update(
                u64::try_from(partition.as_str().len())
                    .map_err(|_| GfError::Execution("partition value is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(partition.as_str().as_bytes());
        }
    }
    Ok(digest.finalize().into())
}

struct PreparedAnalyzeProjection {
    graph: AdjacencyGraph,
    options: AnalyzeOptions,
    partitions: Option<ResolvedPartitionMap>,
}

fn prepare_analyze_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    options: &AnalyzeOptions,
) -> Result<PreparedAnalyzeProjection, GfError> {
    let options = normalize_analyze_options(options)?;
    if matches!(
        options.by,
        AnalyzeAlgorithm::MinimumSpanningTree
            | AnalyzeAlgorithm::MaximumSpanningTree
            | AnalyzeAlgorithm::MinimumKSpanningTree
            | AnalyzeAlgorithm::ArticulationPoints
            | AnalyzeAlgorithm::Bridges
            | AnalyzeAlgorithm::ChromaticNumber
            | AnalyzeAlgorithm::EdgeColoring
            | AnalyzeAlgorithm::TriangleCount
            | AnalyzeAlgorithm::Transitivity
            | AnalyzeAlgorithm::IsPlanar
            | AnalyzeAlgorithm::K1Coloring
            | AnalyzeAlgorithm::NodeColoring
            | AnalyzeAlgorithm::Conductance
            | AnalyzeAlgorithm::Modularity
            | AnalyzeAlgorithm::MaxBipartiteMatching
            | AnalyzeAlgorithm::MaxCardinalityMatching
            | AnalyzeAlgorithm::MaxWeightMatching
    ) && options.directed
    {
        return Err(GfError::Validation(format!(
            "{} requires directed=false",
            options.by
        )));
    }
    if matches!(
        options.by,
        AnalyzeAlgorithm::TopologicalSort
            | AnalyzeAlgorithm::DagLongestPath
            | AnalyzeAlgorithm::DagLongestPathWeighted
            | AnalyzeAlgorithm::TriadCensus
            | AnalyzeAlgorithm::DyadCensus
    ) && !options.directed
    {
        return Err(GfError::Validation(format!(
            "{} requires directed=true",
            options.by
        )));
    }
    if !matches!(
        options.by,
        AnalyzeAlgorithm::MinimumSpanningTree
            | AnalyzeAlgorithm::MaximumSpanningTree
            | AnalyzeAlgorithm::MinimumKSpanningTree
            | AnalyzeAlgorithm::DagLongestPathWeighted
            | AnalyzeAlgorithm::Conductance
            | AnalyzeAlgorithm::Modularity
            | AnalyzeAlgorithm::MaxWeightMatching
    ) && options.weight.is_some()
    {
        return Err(GfError::Validation(format!(
            "{} does not accept an edge weight property",
            options.by
        )));
    }
    if options.by == AnalyzeAlgorithm::DagLongestPathWeighted && options.weight.is_none() {
        return Err(GfError::Validation(
            "dag_longest_path_weighted requires an edge weight property".into(),
        ));
    }
    let via = options.via.as_deref().unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid analyze relationship selector {via:?}"
        )));
    }
    if let Some(weight) = options.weight.as_deref()
        && (weight.is_empty() || weight.trim() != weight || weight.chars().any(char::is_control))
    {
        return Err(GfError::Validation(format!(
            "invalid analyze weight property {weight:?}"
        )));
    }
    let graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via,
            direction: if options.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: options.weight.as_deref(),
        },
    )?;
    let partitions = options
        .partition_property
        .as_deref()
        .map(|property| load_node_partition_property(&graph, dir, property))
        .transpose()?;
    Ok(PreparedAnalyzeProjection {
        graph,
        options,
        partitions,
    })
}

/// Execute one typed embedding analysis through its Rust-owned kernel.
///
/// # Errors
/// Returns structured validation, projection, resource, kernel, or Arrow-shaping
/// failures. Embedding values without an activated native kernel remain
/// unavailable.
pub fn embedding_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<RecordBatch, GfError> {
    embedding_algorithm_execution(provider, dir, mode, label, None, invocation)
        .map(|execution| execution.result)
}

/// Execute an activated embedding and return its neutral deterministic invocation descriptor.
///
/// `label_name` records the normalized public selector corresponding to the
/// already-resolved `label` ID. It does not participate in graph resolution.
pub fn embedding_algorithm_execution(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<EmbeddingExecution, GfError> {
    embedding_algorithm_execution_with_compute(
        provider,
        dir,
        mode,
        label,
        label_name,
        invocation,
        AlgorithmLimits::default(),
        None,
    )
}

/// Execute an embedding with shaping/compute limits and an optional private pool (#344).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors embedding_algorithm_execution plus instance compute handles"
)]
pub fn embedding_algorithm_execution_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<EmbeddingExecution, GfError> {
    let prepared =
        prepare_embedding_projection(provider, dir, mode, label, invocation, limits, compute)?;
    let invocation = &prepared.invocation;
    embedding_algorithm_execution_with_controls(
        &prepared.graph,
        invocation,
        EmbeddingProjectionSelector {
            label: label_name.map(str::to_owned),
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        &prepared.algorithm_control,
        prepared.resource_limits,
        prepared.hashgnn_type_tokens.as_ref(),
    )
}

struct PreparedEmbeddingProjection {
    invocation: NormalizedEmbeddingOptions,
    graph: AdjacencyGraph,
    hashgnn_type_tokens: Option<HashGnnTypeTokens>,
    algorithm_control: AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
}

fn prepare_embedding_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<PreparedEmbeddingProjection, GfError> {
    let invocation = normalize_embedding_options(invocation)?;
    let mut graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via: invocation.via.as_deref().unwrap_or("*"),
            direction: if invocation.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: invocation.weight.as_deref(),
        },
    )?;
    if let EmbeddingOptions::FastRandomProjection(options) = &invocation.options {
        load_node_scalar_features(&mut graph, dir, &options.feature_properties)?;
    }
    if let EmbeddingOptions::GraphSage(options) = &invocation.options {
        load_node_feature_properties(&mut graph, dir, &options.feature_properties)?;
    }
    let hashgnn_type_tokens = match &invocation.options {
        EmbeddingOptions::HashGnn(options) if options.heterogeneous => {
            Some(load_hashgnn_type_tokens(
                &graph,
                dir,
                options
                    .node_type_property
                    .as_deref()
                    .expect("normalized heterogeneous HashGNN has a node type property"),
                options
                    .relationship_type_property
                    .as_deref()
                    .expect("normalized heterogeneous HashGNN has a relationship type property"),
            )?)
        }
        _ => None,
    };
    let mut algorithm_control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        algorithm_control = algorithm_control.with_compute_pool(pool);
    }
    let resource_limits = EmbeddingResourceLimits::default();
    if let EmbeddingOptions::HashGnn(options) = &invocation.options {
        let topology = TopologyResources {
            nodes: usize_to_u64(graph.node_ids().len())?,
            adjacency_entries: graph.edge_entry_count(),
            bytes_per_node: 16,
            bytes_per_adjacency_entry: 32,
        };
        preflight_hashgnn(
            options,
            topology,
            hashgnn_type_tokens
                .as_ref()
                .map(hashgnn_type_token_bytes)
                .transpose()?
                .unwrap_or(0),
            &EmbeddingControl::new(&algorithm_control, resource_limits),
        )?;
    }
    Ok(PreparedEmbeddingProjection {
        invocation,
        graph,
        hashgnn_type_tokens,
        algorithm_control,
        resource_limits,
    })
}

/// Prepare the complete neutral embedding descriptor without running a kernel.
///
/// # Errors
/// Returns the same normalization, projection, property, and resource failures
/// that would occur before [`embedding_algorithm_execution`] starts a kernel.
pub fn prepare_embedding_invocation_descriptor(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<EmbeddingInvocationDescriptor, GfError> {
    prepare_embedding_invocation_descriptor_with_compute(
        provider,
        dir,
        mode,
        label,
        label_name,
        invocation,
        AlgorithmLimits::default(),
        None,
    )
}

/// Prepare an embedding descriptor with the instance compute budget recorded (#344).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors prepare_embedding_invocation_descriptor plus compute handles"
)]
pub fn prepare_embedding_invocation_descriptor_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: Option<TypeId>,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<EmbeddingInvocationDescriptor, GfError> {
    let prepared =
        prepare_embedding_projection(provider, dir, mode, label, invocation, limits, compute)?;
    let invocation = &prepared.invocation;
    let limits = prepared.algorithm_control.configured_limits();
    Ok(EmbeddingInvocationDescriptor {
        catalog_value: match &invocation.options {
            EmbeddingOptions::Node2Vec(_) => "node2vec",
            EmbeddingOptions::GraphSage(_) => "graphsage",
            EmbeddingOptions::FastRandomProjection(_) => "fast_random_projection",
            EmbeddingOptions::HashGnn(_) => "hashgnn",
        },
        algorithm_version: invocation.algorithm_version,
        selector: EmbeddingProjectionSelector {
            label: label_name.map(str::to_owned),
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        options: invocation.options.clone(),
        rng: EmbeddingRngContract {
            version: RNG_VERSION,
            derivation: RNG_DERIVATION,
            seed: invocation.seed(),
        },
        limits: EmbeddingInvocationLimits {
            nodes: limits.nodes,
            adjacency_entries: limits.edges,
            output_rows: limits.output_rows,
            iterations: limits.iterations,
            states: limits.states,
            memory_bytes: prepared.resource_limits.memory_bytes,
            work: prepared.resource_limits.work,
        },
        projection_fingerprint: embedding_descriptor_projection_fingerprint(
            &prepared.graph,
            prepared.hashgnn_type_tokens.as_ref(),
        )?,
        result_schema_version: SCHEMA_VERSION,
    })
}

fn embedding_descriptor_projection_fingerprint(
    graph: &AdjacencyGraph,
    type_tokens: Option<&HashGnnTypeTokens>,
) -> Result<[u8; 32], GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge_embedding_descriptor_projection_v1");
    digest.update(graph.descriptor_projection_fingerprint()?.as_bytes());
    if let Some(tokens) = type_tokens {
        digest.update(b"nodes");
        for (uuid, token) in &tokens.nodes {
            digest.update(uuid);
            digest.update(
                u64::try_from(token.len())
                    .map_err(|_| GfError::Execution("HashGNN token is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(token.as_bytes());
        }
        digest.update(b"relationships");
        for (uuid, token) in &tokens.relationships {
            digest.update(uuid);
            digest.update(
                u64::try_from(token.len())
                    .map_err(|_| GfError::Execution("HashGNN token is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(token.as_bytes());
        }
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
pub(crate) fn embedding_algorithm_with_controls(
    graph: &AdjacencyGraph,
    invocation: &NormalizedEmbeddingOptions,
    algorithm_control: &AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
) -> Result<RecordBatch, GfError> {
    embedding_algorithm_execution_with_controls(
        graph,
        invocation,
        EmbeddingProjectionSelector {
            label: None,
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        algorithm_control,
        resource_limits,
        None,
    )
    .map(|execution| execution.result)
}

fn embedding_algorithm_execution_with_controls(
    graph: &AdjacencyGraph,
    invocation: &NormalizedEmbeddingOptions,
    selector: EmbeddingProjectionSelector,
    algorithm_control: &AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
    hashgnn_type_tokens: Option<&HashGnnTypeTokens>,
) -> Result<EmbeddingExecution, GfError> {
    algorithm_control
        .check_graph_size(graph.node_ids().len(), graph.edge_entry_count())
        .map_err(GfError::from)?;
    algorithm_control
        .check_output_rows(graph.node_ids().len())
        .map_err(GfError::from)?;
    let control = EmbeddingControl::new(algorithm_control, resource_limits);
    let topology = TopologyResources {
        nodes: usize_to_u64(graph.node_ids().len())?,
        adjacency_entries: graph.edge_entry_count(),
        bytes_per_node: 16,
        bytes_per_adjacency_entry: 32,
    };
    let (algorithm, catalog_value, rows) = match &invocation.options {
        EmbeddingOptions::Node2Vec(options) => {
            let estimate = EmbeddingResourceEstimate::node2vec(Node2VecResources {
                topology,
                dimensions: usize_to_u64(options.dimensions)?,
                walks_per_node: usize_to_u64(options.walks_per_node)?,
                walk_length: usize_to_u64(options.walk_length)?,
                window_size: usize_to_u64(options.window_size)?,
                negative_samples: usize_to_u64(options.negative_samples)?,
                epochs: usize_to_u64(options.epochs)?,
                scratch_bytes: 0,
            })
            .map_err(|error| GfError::Execution(error.to_string()))?;
            control
                .preflight(estimate)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let rows = train_node2vec(graph, options, &control)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            (AnalyzeAlgorithm::Node2Vec, "node2vec", rows)
        }
        EmbeddingOptions::FastRandomProjection(options) => {
            let estimate = EmbeddingResourceEstimate::fastrp(FastRpResources {
                topology,
                dimensions: usize_to_u64(options.dimensions)?,
                iteration_weights: usize_to_u64(options.iteration_weights.len())?,
                properties: usize_to_u64(options.feature_properties.len())?,
                scratch_bytes: 0,
            })
            .map_err(|error| GfError::Execution(error.to_string()))?;
            control
                .preflight(estimate)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let rows = train_fastrp(graph, options, &control)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            (
                AnalyzeAlgorithm::FastRandomProjection,
                "fast_random_projection",
                rows,
            )
        }
        EmbeddingOptions::GraphSage(options) => {
            let rows = execute_graphsage(graph, options, topology, &control)?;
            (AnalyzeAlgorithm::GraphSage, "graphsage", rows)
        }
        EmbeddingOptions::HashGnn(options) => {
            let rows = execute_hashgnn(graph, options, topology, hashgnn_type_tokens, &control)?;
            (AnalyzeAlgorithm::HashGnn, "hashgnn", rows)
        }
    };
    let result = shape_embedding_output(algorithm, invocation, &rows, &control)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let limits = algorithm_control.configured_limits();
    Ok(EmbeddingExecution {
        descriptor: EmbeddingInvocationDescriptor {
            catalog_value,
            algorithm_version: invocation.algorithm_version,
            selector,
            options: invocation.options.clone(),
            rng: EmbeddingRngContract {
                version: RNG_VERSION,
                derivation: RNG_DERIVATION,
                seed: invocation.seed(),
            },
            limits: EmbeddingInvocationLimits {
                nodes: limits.nodes,
                adjacency_entries: limits.edges,
                output_rows: limits.output_rows,
                iterations: limits.iterations,
                states: limits.states,
                memory_bytes: resource_limits.memory_bytes,
                work: resource_limits.work,
            },
            projection_fingerprint: embedding_descriptor_projection_fingerprint(
                graph,
                hashgnn_type_tokens,
            )?,
            result_schema_version: SCHEMA_VERSION,
        },
        result,
    })
}

fn usize_to_u64(value: usize) -> Result<u64, GfError> {
    u64::try_from(value).map_err(|_| {
        GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
    })
}

fn execute_hashgnn(
    graph: &AdjacencyGraph,
    options: &graphforge_core::embedding_options::HashGnnOptions,
    topology: TopologyResources,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<crate::algorithm_embedding_output::EmbeddingOutputRow>, GfError> {
    let scratch_bytes = type_tokens
        .map(hashgnn_type_token_bytes)
        .transpose()?
        .unwrap_or(0);
    preflight_hashgnn(options, topology, scratch_bytes, control)?;
    hashgnn_embeddings(graph, options, type_tokens, control)
        .map_err(|error| GfError::Execution(error.to_string()))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "normalized HashGNN dimensions are at most 8192 and density is finite in (0, 1]"
)]
fn preflight_hashgnn(
    options: &graphforge_core::embedding_options::HashGnnOptions,
    topology: TopologyResources,
    scratch_bytes: u64,
    control: &EmbeddingControl<'_>,
) -> Result<(), GfError> {
    let dimensions = usize_to_u64(options.dimensions)?;
    let active_bits = (options.embedding_density * options.dimensions as f64)
        .ceil()
        .max(1.0) as u64;
    let estimate = EmbeddingResourceEstimate::hashgnn(HashGnnResources {
        topology,
        dimensions,
        iterations: usize_to_u64(options.iterations)?,
        active_bits,
        scratch_bytes,
    })
    .map_err(|error| GfError::Execution(error.to_string()))?;
    control
        .preflight(estimate)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn hashgnn_type_token_bytes(tokens: &HashGnnTypeTokens) -> Result<u64, GfError> {
    tokens
        .nodes
        .values()
        .chain(tokens.relationships.values())
        .try_fold(0_u64, |total, token| {
            let token_bytes = usize_to_u64(token.len())?;
            total
                .checked_add(16)
                .and_then(|value| value.checked_add(token_bytes))
                .ok_or_else(|| {
                    GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
                })
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashGnnTypeKind {
    String,
    Integer,
}

fn load_hashgnn_type_tokens(
    graph: &AdjacencyGraph,
    dir: &Path,
    node_property: &str,
    relationship_property: &str,
) -> Result<HashGnnTypeTokens, GfError> {
    let nodes = load_hashgnn_node_types(graph, dir, node_property)?;
    let relationships = load_hashgnn_relationship_types(graph, dir, relationship_property)?;
    Ok(HashGnnTypeTokens {
        nodes,
        relationships,
    })
}

fn load_hashgnn_node_types(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<BTreeMap<[u8; 16], String>, GfError> {
    let selected = graph.node_uuids().collect::<HashSet<_>>();
    let mut values = BTreeMap::new();
    let mut kind = None;
    for stem in graphforge_storage::list_property_stems(dir) {
        for (uuid, row) in graphforge_storage::read_node_property_rows(dir, &stem)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            if !selected.contains(&uuid) {
                continue;
            }
            let Some(value) = row.get(property) else {
                continue;
            };
            let (value_kind, token) = hashgnn_node_type_token(property, &uuid, value)?;
            validate_hashgnn_type_kind("node", property, &mut kind, value_kind)?;
            insert_hashgnn_type_value(&mut values, uuid, token, "node", property)?;
        }
    }
    for uuid in selected {
        if !values.contains_key(&uuid) {
            return Err(GfError::Validation(format!(
                "node {uuid:?} is missing HashGNN type property {property:?}"
            )));
        }
    }
    Ok(values)
}

fn hashgnn_node_type_token(
    property: &str,
    uuid: &[u8; 16],
    value: &IrLiteral,
) -> Result<(HashGnnTypeKind, String), GfError> {
    match value {
        IrLiteral::Str(value) => Ok((
            HashGnnTypeKind::String,
            format!("string:{}:{value}", value.len()),
        )),
        IrLiteral::Int(value) => Ok((HashGnnTypeKind::Integer, format!("integer:{value}"))),
        _ => Err(GfError::Validation(format!(
            "node {uuid:?} HashGNN type property {property:?} must be a non-null scalar string or integer"
        ))),
    }
}

fn load_hashgnn_relationship_types(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<BTreeMap<[u8; 16], String>, GfError> {
    let selected = graph
        .node_ids()
        .iter()
        .flat_map(|&node_id| graph.neighbors(node_id))
        .map(|edge| edge.edge_uuid)
        .collect::<HashSet<_>>();
    let mut values = BTreeMap::new();
    let mut kind = None;
    for stem in graphforge_storage::list_edge_property_stems(dir) {
        for batch in graphforge_storage::read_edge_properties(dir, &stem)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            let Some(uuids) = batch
                .column_by_name("edge_uuid")
                .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
            else {
                return Err(GfError::Execution(
                    "HashGNN edge property batch is missing edge_uuid identity".into(),
                ));
            };
            let Some(column) = batch.column_by_name(property) else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if uuids.is_null(row) || column.is_null(row) {
                    continue;
                }
                let uuid: [u8; 16] = uuids.value(row).try_into().map_err(|_| {
                    GfError::Execution(
                        "HashGNN edge property UUID does not contain 16 bytes".into(),
                    )
                })?;
                if !selected.contains(&uuid) {
                    continue;
                }
                let (value_kind, token) =
                    hashgnn_edge_type_token(property, &uuid, column.as_ref(), row)?;
                validate_hashgnn_type_kind("relationship", property, &mut kind, value_kind)?;
                insert_hashgnn_type_value(&mut values, uuid, token, "relationship", property)?;
            }
        }
    }
    for uuid in selected {
        if !values.contains_key(&uuid) {
            return Err(GfError::Validation(format!(
                "relationship {uuid:?} is missing HashGNN type property {property:?}"
            )));
        }
    }
    Ok(values)
}

fn hashgnn_edge_type_token(
    property: &str,
    uuid: &[u8; 16],
    array: &dyn Array,
    row: usize,
) -> Result<(HashGnnTypeKind, String), GfError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        let value = values.value(row);
        Ok((
            HashGnnTypeKind::String,
            format!("string:{}:{value}", value.len()),
        ))
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        Ok((
            HashGnnTypeKind::Integer,
            format!("integer:{}", values.value(row)),
        ))
    } else {
        Err(GfError::Validation(format!(
            "relationship {uuid:?} HashGNN type property {property:?} must be a non-null scalar string or integer"
        )))
    }
}

fn validate_hashgnn_type_kind(
    entity: &str,
    property: &str,
    expected: &mut Option<HashGnnTypeKind>,
    actual: HashGnnTypeKind,
) -> Result<(), GfError> {
    if expected.is_some_and(|value| value != actual) {
        return Err(GfError::Validation(format!(
            "{entity} HashGNN type property {property:?} mixes string and integer values"
        )));
    }
    *expected = Some(actual);
    Ok(())
}

fn insert_hashgnn_type_value(
    values: &mut BTreeMap<[u8; 16], String>,
    uuid: [u8; 16],
    token: String,
    entity: &str,
    property: &str,
) -> Result<(), GfError> {
    match values.entry(uuid) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(token);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &token => {
            return Err(GfError::Validation(format!(
                "{entity} {uuid:?} has conflicting HashGNN type property {property:?}"
            )));
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    Ok(())
}

fn execute_graphsage(
    graph: &AdjacencyGraph,
    options: &graphforge_core::embedding_options::GraphSageOptions,
    topology: TopologyResources,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<crate::algorithm_embedding_output::EmbeddingOutputRow>, GfError> {
    if graph.is_empty() {
        return Ok(Vec::new());
    }
    let (feature_width, retained_source_bytes) = graphsage_source_resources(graph)?;
    preflight_graphsage_dispatch(
        topology.nodes,
        topology.adjacency_entries,
        feature_width,
        retained_source_bytes,
        options,
        control,
    )
    .map_err(|error| GfError::Execution(error.to_string()))?;
    let projection = graphsage_projection(graph)?;
    train_graphsage(&projection, options, control)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn graphsage_projection(graph: &AdjacencyGraph) -> Result<GraphSageProjection, GfError> {
    let nodes = graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            let uuid = graph.node_uuid(node_id).ok_or_else(|| {
                GfError::Execution("graphsage selected node has no UUID identity".into())
            })?;
            let features = graph.node_vector(node_id).ok_or_else(|| {
                GfError::Validation(format!(
                    "graphsage selected node {uuid:?} has no resolved feature vector"
                ))
            })?;
            Ok(GraphSageNode {
                uuid,
                features: features.to_vec(),
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let mut seen_edges = HashSet::new();
    let mut edges = Vec::new();
    for &source_id in graph.node_ids() {
        let source_uuid = graph.node_uuid(source_id).ok_or_else(|| {
            GfError::Execution("graphsage selected node has no UUID identity".into())
        })?;
        for edge in graph.neighbors(source_id) {
            if !seen_edges.insert(edge.edge_uuid) {
                continue;
            }
            let target_uuid = graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                GfError::Execution("graphsage selected neighbor has no UUID identity".into())
            })?;
            edges.push(GraphSageEdge {
                uuid: edge.edge_uuid,
                source_uuid,
                target_uuid,
            });
        }
    }
    validate_graphsage_projection(nodes, edges)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn graphsage_source_resources(graph: &AdjacencyGraph) -> Result<(u64, u64), GfError> {
    let first_id = *graph
        .node_ids()
        .first()
        .ok_or_else(|| GfError::Execution("graphsage source projection is empty".into()))?;
    let first = graph.node_vector(first_id).ok_or_else(|| {
        GfError::Validation("graphsage selected node has no resolved feature vector".into())
    })?;
    if first.is_empty() {
        return Err(GfError::Validation(
            "graphsage requires a non-empty numeric feature vector".into(),
        ));
    }
    for &node_id in graph.node_ids() {
        let vector = graph.node_vector(node_id).ok_or_else(|| {
            GfError::Validation("graphsage selected node has no resolved feature vector".into())
        })?;
        if vector.len() != first.len() {
            return Err(GfError::Validation(
                "graphsage feature vectors have inconsistent shape".into(),
            ));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(GfError::Validation(
                "graphsage features must be finite".into(),
            ));
        }
    }
    let nodes = usize_to_u64(graph.node_ids().len())?;
    let width = usize_to_u64(first.len())?;
    let topology_bytes = nodes
        .checked_mul(16)
        .and_then(|bytes| {
            graph
                .edge_entry_count()
                .checked_mul(32)
                .and_then(|adjacency| bytes.checked_add(adjacency))
        })
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let feature_bytes = nodes
        .checked_mul(width)
        .and_then(|cells| cells.checked_mul(8))
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let projection_staging_bytes = graph
        .edge_entry_count()
        .checked_mul(usize_to_u64(std::mem::size_of::<GraphSageEdge>())?)
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let retained_source_bytes = topology_bytes
        .checked_add(feature_bytes)
        .and_then(|bytes| bytes.checked_add(projection_staging_bytes))
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    Ok((width, retained_source_bytes))
}

fn spanning_node_uuid(
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

fn bipartite_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_bipartite_matching node has no UUID identity".into(),
        })
}

fn matching_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_weight_matching node has no UUID identity".into(),
        })
}

fn cardinality_matching_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "max_cardinality_matching node has no UUID identity".into(),
        })
}

fn conductance_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    partition_metric_node_uuid(graph, node, "conductance")
}

fn partition_metric_node_uuid(
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

fn node_coloring_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "node_coloring node has no UUID identity".into(),
        })
}

fn k1_coloring_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "k1_coloring node has no UUID identity".into(),
        })
}

fn chromatic_number_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "chromatic_number node has no UUID identity".into(),
        })
}

fn topological_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "topological_sort node has no UUID identity".into(),
        })
}

fn find_cycles_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "find_cycles node has no UUID identity".into(),
        })
}

fn dag_longest_path_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "dag_longest_path node has no UUID identity".into(),
        })
}

fn weighted_dag_longest_path_node_uuid(
    graph: &AdjacencyGraph,
    node: u64,
) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "dag_longest_path_weighted node has no UUID identity".into(),
        })
}

fn edge_coloring_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "edge_coloring node has no UUID identity".into(),
        })
}

fn euler_circuit_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "has_euler_circuit node has no UUID identity".into(),
        })
}

fn euler_path_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "has_euler_path node has no UUID identity".into(),
        })
}

fn bridge_node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| AlgorithmError::Execution {
            message: "bridges endpoint has no UUID identity".into(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_analyzers_checkpoint_on_large_deterministic_projection() {
        let graph = AdjacencyGraph::from_resolved_projection(
            crate::algorithm_graph::ResolvedGraphProjection {
                directed: true,
                nodes: (0_u32..4_097)
                    .map(|value| {
                        let mut uuid = [0_u8; 16];
                        uuid[12..].copy_from_slice(&value.to_be_bytes());
                        uuid
                    })
                    .collect(),
                edges: Vec::new(),
            },
        )
        .unwrap();
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let analyzers: Vec<Box<dyn RustAlgorithm>> = vec![
            Box::new(TriangleCount),
            Box::new(Transitivity),
            Box::new(IsPlanar),
            Box::new(TriadCensus),
            Box::new(DyadCensus),
            Box::new(ArticulationPoints),
            Box::new(Bridges),
            Box::new(TopologicalSort),
            Box::new(IsDag { directed: true }),
            Box::new(FindCycles { directed: true }),
            Box::new(HasEulerCircuit { directed: true }),
            Box::new(HasEulerPath { directed: true }),
            Box::new(DagLongestPath),
        ];
        for analyzer in analyzers {
            analyzer.execute(&graph, &control).unwrap_or_else(|error| {
                panic!(
                    "{:?} failed on an edgeless projection: {error}",
                    analyzer.capability().algorithm
                )
            });
        }
    }

    #[test]
    fn malformed_adjacency_is_rejected_by_every_analysis_projection_adapter() {
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let missing = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0, 1],
            HashMap::from([(0, [1; 16]), (1, [2; 16])]),
            HashMap::from([(
                0,
                vec![crate::algorithm_graph::AlgorithmEdge {
                    edge_id: 1,
                    edge_uuid: [9; 16],
                    neighbor_id: 2,
                    weight: 1.0,
                }],
            )]),
        );
        let empty_partitions = ResolvedPartitionMap::try_new(
            [[1; 16]],
            [(
                [1; 16],
                crate::algorithm_partition::PartitionValue::String("a".into()),
            )],
        )
        .unwrap();
        let adapters: Vec<Box<dyn RustAlgorithm>> = vec![
            Box::new(CountAutomorphisms { directed: true }),
            Box::new(Conductance {
                partitions: empty_partitions.clone(),
            }),
            Box::new(Modularity {
                partitions: empty_partitions.clone(),
            }),
            Box::new(MaxBipartiteMatching {
                partitions: Some(empty_partitions),
            }),
            Box::new(TriangleCount),
            Box::new(Transitivity),
            Box::new(IsPlanar),
            Box::new(TriadCensus),
            Box::new(DyadCensus),
        ];
        for adapter in adapters {
            assert!(
                adapter.execute(&missing, &control).is_err(),
                "{:?}",
                adapter.capability().algorithm
            );
        }

        let edge = |neighbor_id| crate::algorithm_graph::AlgorithmEdge {
            edge_id: neighbor_id,
            edge_uuid: [7; 16],
            neighbor_id,
            weight: 1.0,
        };
        let inconsistent = AdjacencyGraph::malformed_for_defensive_tests(
            false,
            vec![0, 1],
            HashMap::from([(0, [1; 16]), (1, [2; 16])]),
            HashMap::from([(0, vec![edge(0), edge(1)])]),
        );
        let partitions = ResolvedPartitionMap::try_new(
            [[1; 16], [2; 16]],
            [
                (
                    [1; 16],
                    crate::algorithm_partition::PartitionValue::String("a".into()),
                ),
                (
                    [2; 16],
                    crate::algorithm_partition::PartitionValue::String("b".into()),
                ),
            ],
        )
        .unwrap();
        for adapter in [
            Box::new(CountAutomorphisms { directed: false }) as Box<dyn RustAlgorithm>,
            Box::new(Conductance {
                partitions: partitions.clone(),
            }),
            Box::new(Modularity {
                partitions: partitions.clone(),
            }),
            Box::new(MaxBipartiteMatching {
                partitions: Some(partitions),
            }),
        ] {
            assert!(
                adapter.execute(&inconsistent, &control).is_err(),
                "{:?}",
                adapter.capability().algorithm
            );
        }
        assert!(
            automorphism_allocation("test")
                .to_string()
                .contains("test allocation failed")
        );
    }

    #[test]
    fn missing_uuid_helpers_preserve_algorithm_specific_errors() {
        let graph = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::new(),
            HashMap::new(),
        );
        let errors = [
            spanning_node_uuid(&graph, 0, AnalyzeAlgorithm::MinimumSpanningTree).unwrap_err(),
            bipartite_node_uuid(&graph, 0).unwrap_err(),
            matching_node_uuid(&graph, 0).unwrap_err(),
            cardinality_matching_node_uuid(&graph, 0).unwrap_err(),
            conductance_node_uuid(&graph, 0).unwrap_err(),
            node_coloring_node_uuid(&graph, 0).unwrap_err(),
            k1_coloring_node_uuid(&graph, 0).unwrap_err(),
            chromatic_number_node_uuid(&graph, 0).unwrap_err(),
            topological_node_uuid(&graph, 0).unwrap_err(),
            find_cycles_node_uuid(&graph, 0).unwrap_err(),
            dag_longest_path_node_uuid(&graph, 0).unwrap_err(),
            weighted_dag_longest_path_node_uuid(&graph, 0).unwrap_err(),
            edge_coloring_node_uuid(&graph, 0).unwrap_err(),
            euler_circuit_node_uuid(&graph, 0).unwrap_err(),
            euler_path_node_uuid(&graph, 0).unwrap_err(),
            bridge_node_uuid(&graph, 0).unwrap_err(),
        ];
        for error in errors {
            assert!(error.to_string().contains("UUID identity"));
        }
    }

    #[test]
    fn graphsage_and_dag_adapters_reject_incomplete_projection_identity() {
        let mut missing_uuid = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::new(),
            HashMap::new(),
        );
        missing_uuid
            .replace_node_vectors(HashMap::from([(0, vec![1.0])]))
            .unwrap();
        assert!(
            graphsage_projection(&missing_uuid)
                .unwrap_err()
                .to_string()
                .contains("selected node has no UUID identity")
        );

        let missing_vector = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::from([(0, [1; 16])]),
            HashMap::new(),
        );
        assert!(
            graphsage_projection(&missing_vector)
                .unwrap_err()
                .to_string()
                .contains("no resolved feature vector")
        );

        let edge = crate::algorithm_graph::AlgorithmEdge {
            edge_id: 1,
            edge_uuid: [9; 16],
            neighbor_id: 1,
            weight: 1.0,
        };
        let mut missing_neighbor = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::from([(0, [1; 16])]),
            HashMap::from([(0, vec![edge])]),
        );
        missing_neighbor
            .replace_node_vectors(HashMap::from([(0, vec![1.0])]))
            .unwrap();
        assert!(
            graphsage_projection(&missing_neighbor)
                .unwrap_err()
                .to_string()
                .contains("selected neighbor has no UUID identity")
        );
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        assert!(
            directed_is_dag(&missing_neighbor, &control)
                .unwrap_err()
                .to_string()
                .contains("unselected node")
        );
    }
    use crate::algorithm_partition::PartitionValue;

    fn node2vec_invocation(
        options: graphforge_core::embedding_options::Node2VecOptions,
    ) -> EmbeddingAnalyzeOptions {
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::Node2Vec,
            via: None,
            directed: true,
            weight: None,
            options: EmbeddingOptions::Node2Vec(options),
        }
    }

    fn execute_node2vec_with_controls(
        graph: &AdjacencyGraph,
        options: graphforge_core::embedding_options::Node2VecOptions,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
        resource_limits: EmbeddingResourceLimits,
    ) -> Result<RecordBatch, GfError> {
        let invocation = normalize_embedding_options(&node2vec_invocation(options))?;
        let control = AlgorithmControl::new(limits, cancellation);
        embedding_algorithm_with_controls(graph, &invocation, &control, resource_limits)
    }

    fn fastrp_invocation(
        options: graphforge_core::embedding_options::FastRpOptions,
    ) -> EmbeddingAnalyzeOptions {
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::FastRandomProjection,
            via: None,
            directed: true,
            weight: None,
            options: EmbeddingOptions::FastRandomProjection(options),
        }
    }

    fn graphsage_invocation(
        options: graphforge_core::embedding_options::GraphSageOptions,
    ) -> EmbeddingAnalyzeOptions {
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::GraphSage,
            via: None,
            directed: false,
            weight: None,
            options: EmbeddingOptions::GraphSage(options),
        }
    }

    fn hashgnn_invocation(
        options: graphforge_core::embedding_options::HashGnnOptions,
    ) -> EmbeddingAnalyzeOptions {
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::HashGnn,
            via: None,
            directed: true,
            weight: None,
            options: EmbeddingOptions::HashGnn(options),
        }
    }

    #[test]
    fn node2vec_descriptor_is_deterministic_and_persistence_independent() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let options = graphforge_core::embedding_options::Node2VecOptions {
            dimensions: 4,
            walk_length: 3,
            walks_per_node: 2,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            seed: 7,
            ..graphforge_core::embedding_options::Node2VecOptions::default()
        };
        let invocation =
            normalize_embedding_options(&node2vec_invocation(options.clone())).unwrap();
        let selector = EmbeddingProjectionSelector {
            label: Some("Person".into()),
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
        };
        let execute = || {
            embedding_algorithm_execution_with_controls(
                &graph,
                &invocation,
                selector.clone(),
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
                EmbeddingResourceLimits::default(),
                None,
            )
            .unwrap()
        };

        let first = execute();
        assert_eq!(
            first.descriptor.options,
            EmbeddingOptions::Node2Vec(options)
        );
        assert_eq!(first.descriptor.selector, selector);
        assert_eq!(first.descriptor.rng.seed, 7);
        assert_eq!(
            first.descriptor.projection_fingerprint,
            embedding_descriptor_projection_fingerprint(&graph, None).unwrap()
        );

        let directory = tempfile::tempdir().unwrap();
        let persisted = directory.path().join("invocation.bin");
        std::fs::write(&persisted, first.descriptor.canonical_bytes()).unwrap();
        let stored = std::fs::read(&persisted).unwrap();

        let second = execute();
        assert_eq!(stored, second.descriptor.canonical_bytes());
        assert_eq!(first.descriptor, second.descriptor);
        assert_eq!(first.result, second.result);
    }

    #[test]
    fn public_embedding_facade_executes_an_empty_persisted_projection() {
        let project = tempfile::tempdir().unwrap();
        let provider = crate::adjacency::ScanBuildAdjacencyProvider::new(
            project.path().to_path_buf(),
            OntologyMode::Exploratory,
        );
        let invocation =
            node2vec_invocation(graphforge_core::embedding_options::Node2VecOptions::default());

        let result = embedding_algorithm(
            &provider,
            project.path(),
            OntologyMode::Exploratory,
            None,
            &invocation,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn node2vec_dispatch_resource_failures_are_structured_and_atomic() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let options = graphforge_core::embedding_options::Node2VecOptions {
            dimensions: 4,
            walk_length: 3,
            walks_per_node: 2,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            seed: 7,
            ..graphforge_core::embedding_options::Node2VecOptions::default()
        };
        let output = execute_node2vec_with_controls(
            &graph,
            options.clone(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .expect("control invocation");
        assert_eq!(output.num_rows(), 3);

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        for (name, result, expected) in [
            (
                "cancellation",
                execute_node2vec_with_controls(
                    &graph,
                    options.clone(),
                    AlgorithmLimits::default(),
                    cancelled,
                    EmbeddingResourceLimits::default(),
                ),
                "algorithm execution cancelled",
            ),
            (
                "node limit",
                execute_node2vec_with_controls(
                    &graph,
                    options.clone(),
                    AlgorithmLimits {
                        nodes: 2,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits::default(),
                ),
                "algorithm node limit exceeded",
            ),
            (
                "output limit",
                execute_node2vec_with_controls(
                    &graph,
                    options.clone(),
                    AlgorithmLimits {
                        output_rows: 2,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits::default(),
                ),
                "algorithm output row limit exceeded",
            ),
            (
                "memory limit",
                execute_node2vec_with_controls(
                    &graph,
                    options.clone(),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits {
                        memory_bytes: 0,
                        work: u64::MAX,
                    },
                ),
                "embedding memory limit exceeded",
            ),
            (
                "work limit",
                execute_node2vec_with_controls(
                    &graph,
                    options.clone(),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits {
                        memory_bytes: u64::MAX,
                        work: 0,
                    },
                ),
                "embedding work limit exceeded",
            ),
            (
                "resource overflow",
                execute_node2vec_with_controls(
                    &graph,
                    graphforge_core::embedding_options::Node2VecOptions {
                        walks_per_node: usize::MAX,
                        ..options
                    },
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits {
                        memory_bytes: u64::MAX,
                        work: u64::MAX,
                    },
                ),
                "embedding resource accounting exceeds UInt64 range",
            ),
        ] {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{name} returned unexpected error: {error}"
            );
        }
    }

    #[test]
    fn fastrp_descriptor_replays_and_dispatch_controls_fail_atomically() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let options = graphforge_core::embedding_options::FastRpOptions {
            dimensions: 4,
            iteration_weights: vec![1.0, 1.0],
            seed: 11,
            ..graphforge_core::embedding_options::FastRpOptions::default()
        };
        let invocation = normalize_embedding_options(&fastrp_invocation(options.clone())).unwrap();
        let selector = EmbeddingProjectionSelector {
            label: Some("Person".into()),
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
        };
        let execute = |cancellation, resource_limits| {
            embedding_algorithm_execution_with_controls(
                &graph,
                &invocation,
                selector.clone(),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                resource_limits,
                None,
            )
        };
        let first = execute(
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        let second = execute(
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        assert_eq!(
            first.descriptor.options,
            EmbeddingOptions::FastRandomProjection(options)
        );
        assert_eq!(first.descriptor.selector, selector);
        assert_eq!(
            first.descriptor.canonical_bytes(),
            second.descriptor.canonical_bytes()
        );
        assert_eq!(first.result, second.result);

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        assert!(matches!(
            execute(cancelled, EmbeddingResourceLimits::default()),
            Err(GfError::Execution(message)) if message.contains("cancel")
        ));
        assert!(matches!(
            execute(
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: 0,
                    work: u64::MAX,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("memory")
        ));
        assert!(matches!(
            execute(
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: u64::MAX,
                    work: 0,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("work")
        ));
    }

    #[test]
    fn graphsage_descriptor_replays_and_dispatch_controls_fail_atomically() {
        let mut graph =
            AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)]);
        graph
            .replace_node_vectors(HashMap::from([
                (0, vec![1.0, 0.0]),
                (1, vec![0.0, 1.0]),
                (2, vec![0.5, 0.5]),
            ]))
            .unwrap();
        let options = graphforge_core::embedding_options::GraphSageOptions {
            dimensions: 2,
            hidden_dimensions: 2,
            layers: 1,
            sample_sizes: vec![1],
            epochs: 1,
            negative_samples: 1,
            learning_rate: 0.001,
            feature_properties: vec!["features".into()],
            seed: 13,
            ..graphforge_core::embedding_options::GraphSageOptions::default()
        };
        let invocation =
            normalize_embedding_options(&graphsage_invocation(options.clone())).unwrap();
        let selector = EmbeddingProjectionSelector {
            label: Some("Person".into()),
            via: Some("KNOWS".into()),
            directed: false,
            weight: None,
        };
        let execute = |limits, cancellation, resource_limits| {
            embedding_algorithm_execution_with_controls(
                &graph,
                &invocation,
                selector.clone(),
                &AlgorithmControl::new(limits, cancellation),
                resource_limits,
                None,
            )
        };
        let first = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let persisted_path = directory.path().join("graphsage-invocation.bin");
        std::fs::write(&persisted_path, first.descriptor.canonical_bytes()).unwrap();
        let persisted = std::fs::read(&persisted_path).unwrap();
        let second = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        assert_eq!(
            first.descriptor.options,
            EmbeddingOptions::GraphSage(options)
        );
        assert_eq!(first.descriptor.selector, selector);
        assert_eq!(first.descriptor.rng.seed, 13);
        assert_eq!(persisted, second.descriptor.canonical_bytes());
        assert_eq!(first.result, second.result);

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                cancelled,
                EmbeddingResourceLimits::default()
            ),
            Err(GfError::Execution(message)) if message.contains("cancel")
        ));
        for (limits, expected) in [
            (
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                "node limit",
            ),
            (
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                "output row limit",
            ),
            (
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                "iteration limit",
            ),
        ] {
            assert!(matches!(
                execute(
                    limits,
                    AlgorithmCancellation::default(),
                    EmbeddingResourceLimits::default()
                ),
                Err(GfError::Execution(message)) if message.contains(expected)
            ));
        }
        let memory_error = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: 0,
                work: u64::MAX,
            },
        )
        .unwrap_err()
        .to_string();
        let observed = memory_error
            .split_once("observed ")
            .and_then(|(_, suffix)| suffix.split_once(','))
            .and_then(|(value, _)| value.parse::<u64>().ok())
            .expect("structured GraphSAGE memory error reports observed bytes");
        assert!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: observed,
                    work: u64::MAX,
                },
            )
            .is_ok()
        );
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: observed - 1,
                    work: u64::MAX,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("memory")
        ));
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: u64::MAX,
                    work: 0,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("work")
        ));
    }

    #[test]
    fn hashgnn_descriptor_replays_and_dispatch_controls_fail_atomically() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let options = graphforge_core::embedding_options::HashGnnOptions {
            dimensions: 8,
            iterations: 2,
            embedding_density: 0.25,
            heterogeneous: true,
            node_type_property: Some("kind".into()),
            relationship_type_property: Some("kind".into()),
            seed: 19,
            ..graphforge_core::embedding_options::HashGnnOptions::default()
        };
        let type_tokens = HashGnnTypeTokens {
            nodes: BTreeMap::from([
                (0_u128.to_be_bytes(), "string:5:human".into()),
                (1_u128.to_be_bytes(), "string:5:human".into()),
                (2_u128.to_be_bytes(), "string:5:human".into()),
            ]),
            relationships: BTreeMap::from([
                (0_u128.to_be_bytes(), "string:6:friend".into()),
                (1_u128.to_be_bytes(), "string:6:friend".into()),
            ]),
        };
        let invocation = normalize_embedding_options(&hashgnn_invocation(options.clone())).unwrap();
        let selector = EmbeddingProjectionSelector {
            label: Some("Person".into()),
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
        };
        let execute = |limits, cancellation, resource_limits| {
            embedding_algorithm_execution_with_controls(
                &graph,
                &invocation,
                selector.clone(),
                &AlgorithmControl::new(limits, cancellation),
                resource_limits,
                Some(&type_tokens),
            )
        };
        let first = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let persisted_path = directory.path().join("hashgnn-invocation.bin");
        std::fs::write(&persisted_path, first.descriptor.canonical_bytes()).unwrap();
        let persisted = std::fs::read(&persisted_path).unwrap();
        let second = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap();
        assert_eq!(first.descriptor.options, EmbeddingOptions::HashGnn(options));
        assert_eq!(first.descriptor.selector, selector);
        assert_eq!(first.descriptor.rng.seed, 19);
        assert_eq!(persisted, second.descriptor.canonical_bytes());
        assert_eq!(first.result, second.result);

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                cancelled,
                EmbeddingResourceLimits::default()
            ),
            Err(GfError::Execution(message)) if message.contains("cancel")
        ));
        assert!(matches!(
            execute(
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits::default(),
            ),
            Err(GfError::Execution(message)) if message.contains("iteration limit")
        ));
        let memory_error = execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: 0,
                work: u64::MAX,
            },
        )
        .unwrap_err()
        .to_string();
        let observed = memory_error
            .split_once("observed ")
            .and_then(|(_, suffix)| suffix.split_once(','))
            .and_then(|(value, _)| value.parse::<u64>().ok())
            .expect("structured HashGNN memory error reports observed bytes");
        assert!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: observed,
                    work: u64::MAX,
                },
            )
            .is_ok()
        );
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: observed - 1,
                    work: u64::MAX,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("memory")
        ));
        assert!(matches!(
            execute(
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: u64::MAX,
                    work: 0,
                },
            ),
            Err(GfError::Execution(message)) if message.contains("work")
        ));
    }

    #[test]
    fn hashgnn_type_tokens_are_canonical_and_reject_invalid_or_conflicting_values() {
        let uuid = 7_u128.to_be_bytes();
        assert_eq!(
            hashgnn_node_type_token("kind", &uuid, &IrLiteral::Str("hé".into())).unwrap(),
            (HashGnnTypeKind::String, "string:3:hé".into())
        );
        assert_eq!(
            hashgnn_node_type_token("kind", &uuid, &IrLiteral::Int(-7)).unwrap(),
            (HashGnnTypeKind::Integer, "integer:-7".into())
        );
        assert!(matches!(
            hashgnn_node_type_token("kind", &uuid, &IrLiteral::Null),
            Err(GfError::Validation(message)) if message.contains("non-null scalar")
        ));

        let strings = StringArray::from(vec!["friend"]);
        assert_eq!(
            hashgnn_edge_type_token("kind", &uuid, &strings, 0).unwrap(),
            (HashGnnTypeKind::String, "string:6:friend".into())
        );
        let integers = Int64Array::from(vec![9]);
        assert_eq!(
            hashgnn_edge_type_token("kind", &uuid, &integers, 0).unwrap(),
            (HashGnnTypeKind::Integer, "integer:9".into())
        );
        let unsupported = arrow::array::BooleanArray::from(vec![true]);
        assert!(matches!(
            hashgnn_edge_type_token("kind", &uuid, &unsupported, 0),
            Err(GfError::Validation(message)) if message.contains("non-null scalar")
        ));

        let mut kind = None;
        validate_hashgnn_type_kind("node", "kind", &mut kind, HashGnnTypeKind::String).unwrap();
        assert!(matches!(
            validate_hashgnn_type_kind(
                "node",
                "kind",
                &mut kind,
                HashGnnTypeKind::Integer
            ),
            Err(GfError::Validation(message)) if message.contains("mixes string and integer")
        ));

        let mut values = BTreeMap::new();
        insert_hashgnn_type_value(&mut values, uuid, "string:5:human".into(), "node", "kind")
            .unwrap();
        assert!(matches!(
            insert_hashgnn_type_value(
                &mut values,
                uuid,
                "string:6:person".into(),
                "node",
                "kind"
            ),
            Err(GfError::Validation(message)) if message.contains("conflicting")
        ));
    }

    #[test]
    fn minimum_k_spanning_tree_normalizes_and_validates_k() {
        let defaults = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumKSpanningTree,
            directed: false,
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(defaults.k, Some(1));

        let explicit = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumKSpanningTree,
            directed: false,
            k: Some(3),
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(explicit.k, Some(3));

        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::MinimumKSpanningTree,
                directed: false,
                k: Some(0),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message)
                if message == "minimum_k_spanning_tree requires k greater than zero"
        ));
    }

    #[test]
    fn k_is_rejected_for_other_analyze_algorithms() {
        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                k: Some(2),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message) if message == "is_dag does not accept k"
        ));
    }

    #[test]
    fn partition_property_validation_matches_partition_algorithms() {
        let inferred = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MaxBipartiteMatching,
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(inferred.partition_property, None);

        let explicit = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MaxBipartiteMatching,
            partition_property: Some("side".into()),
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(explicit.partition_property.as_deref(), Some("side"));

        for by in [
            AnalyzeAlgorithm::MaxBipartiteMatching,
            AnalyzeAlgorithm::Conductance,
            AnalyzeAlgorithm::Modularity,
        ] {
            assert!(matches!(
                normalize_analyze_options(&AnalyzeOptions {
                    by,
                    partition_property: Some(String::new()),
                    ..AnalyzeOptions::default()
                })
                .unwrap_err(),
                GfError::Validation(message) if message.contains("requires a non-empty partition_property")
            ));
        }

        assert!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::Conductance,
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .is_ok()
        );
        assert!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .is_ok()
        );

        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message)
                if message == "is_dag does not accept partition_property"
        ));
    }

    fn execute(
        graph: &AdjacencyGraph,
        algorithm: AnalyzeAlgorithm,
        directed: bool,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, directed)?;
        if algorithm == AnalyzeAlgorithm::MaxBipartiteMatching {
            registry.register(Arc::new(MaxBipartiteMatching { partitions: None }))?;
        }
        registry.execute(
            Algorithm::Analyze(algorithm),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_with_compute_threads(
        graph: &AdjacencyGraph,
        algorithm: AnalyzeAlgorithm,
        directed: bool,
        threads: usize,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, directed)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
        registry.execute(Algorithm::Analyze(algorithm), graph, &control)
    }

    fn output_fingerprint(output: &AlgorithmOutput) -> String {
        format!("{:?}|{:?}", output.schema, output.rows())
    }

    fn execute_minimum_k_spanning_tree(
        graph: &AdjacencyGraph,
        k: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let options = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumKSpanningTree,
            directed: false,
            k: Some(k),
            ..AnalyzeOptions::default()
        })
        .expect("test k is valid");
        let mut registry = AlgorithmRegistry::default();
        register_option_analyze_algorithm(&mut registry, &options, None)
            .expect("minimum-k registration does not read external options");
        registry.execute(
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_automorphism_count(
        graph: &AdjacencyGraph,
        directed: bool,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute(
            graph,
            AnalyzeAlgorithm::CountAutomorphisms,
            directed,
            limits,
            cancellation,
        )
    }

    fn automorphism_count(graph: &AdjacencyGraph, directed: bool) -> u64 {
        let output = execute_automorphism_count(
            graph,
            directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms).result_schema()
        );
        let rows = output.rows();
        let [row] = rows.as_slice() else {
            panic!("automorphism dispatch must return exactly one row");
        };
        let [AlgorithmValue::UInt64(count)] = row.as_slice() else {
            panic!("automorphism dispatch must return one UInt64 value");
        };
        *count
    }

    #[test]
    fn automorphism_dispatch_counts_canonical_graph_families_and_schema() {
        assert_eq!(
            automorphism_count(&AdjacencyGraph::with_test_edges(0, &[]), false),
            1
        );
        assert_eq!(
            automorphism_count(&AdjacencyGraph::with_test_edges(1, &[]), false),
            1
        );
        assert_eq!(
            automorphism_count(
                &AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)],),
                false,
            ),
            2
        );
        assert_eq!(
            automorphism_count(
                &AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
                true,
            ),
            3
        );
        assert_eq!(
            automorphism_count(
                &AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 0), (11, 0, 1)],),
                false,
            ),
            1
        );
        assert_eq!(
            automorphism_count(
                &AdjacencyGraph::with_test_undirected_multigraph(
                    3,
                    &[(10, 0, 1), (11, 0, 1), (12, 0, 2)],
                ),
                false,
            ),
            1
        );
    }

    #[test]
    fn automorphism_dispatch_is_repeatable_uuid_rename_invariant_and_registered() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        let renamed = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[
                90_u128.to_be_bytes(),
                2_u128.to_be_bytes(),
                70_u128.to_be_bytes(),
            ],
            &[(0, 1), (1, 2), (2, 0)],
        );
        assert_eq!(automorphism_count(&graph, true), 3);
        assert_eq!(
            automorphism_count(&graph, true),
            automorphism_count(&renamed, true)
        );

        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, true).unwrap();
        assert_eq!(
            registry
                .capabilities()
                .into_iter()
                .filter(|capability| {
                    capability.algorithm == Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms)
                })
                .count(),
            1
        );
    }

    #[test]
    fn automorphism_dispatch_propagates_cancellation_and_resource_limits_atomically() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_automorphism_count(&graph, false, AlgorithmLimits::default(), cancellation,),
            Err(AlgorithmError::Cancelled)
        );
        assert_eq!(
            execute_automorphism_count(
                &graph,
                false,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            })
        );
        assert!(matches!(
            execute_automorphism_count(
                &graph,
                false,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert_eq!(
            execute_automorphism_count(
                &graph,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit {
                observed: 1,
                limit: 0,
            })
        );
    }

    fn conductance_partitions(
        graph: &AdjacencyGraph,
        values: &[(u8, &str)],
    ) -> ResolvedPartitionMap {
        ResolvedPartitionMap::try_new(
            graph.node_uuids(),
            values.iter().map(|&(node, partition)| {
                (
                    u128::from(node).to_be_bytes(),
                    PartitionValue::String(partition.into()),
                )
            }),
        )
        .unwrap()
    }

    #[test]
    fn conductance_dispatches_weighted_rows_with_stable_schema() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 0, 2), (11, 1, 2), (12, 0, 1), (13, 3, 3)],
        )
        .with_test_edge_weights(&[2.0, 2.0, 1.0, 1.0, 3.0, 3.0, 4.0]);
        let handler = Conductance {
            partitions: conductance_partitions(
                &graph,
                &[(0, "alpha"), (1, "alpha"), (2, "beta"), (3, "beta")],
            ),
        };
        let output = handler
            .execute(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
            )
            .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::Conductance).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Utf8("alpha".into()),
                    AlgorithmValue::Float64(1.0 / 3.0),
                ],
                vec![
                    AlgorithmValue::Utf8("beta".into()),
                    AlgorithmValue::Float64(1.0 / 3.0),
                ],
            ]
        );
        let batch =
            shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::Conductance), &output)
                .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [("partition_id", false), ("conductance", false)]
        );
    }

    #[test]
    fn modularity_dispatches_weighted_scalar_with_stable_schema() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 0, 1), (11, 2, 3), (12, 1, 2), (13, 0, 0)],
        )
        .with_test_edge_weights(&[2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 3.0]);
        let handler = Modularity {
            partitions: conductance_partitions(
                &graph,
                &[(0, "alpha"), (1, "alpha"), (2, "beta"), (3, "beta")],
            ),
        };
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let output = handler.execute(&graph, &control).unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::Modularity).result_schema()
        );
        assert_eq!(output.rows().len(), 1);
        assert!(
            matches!(output.rows()[0].as_slice(), [AlgorithmValue::Float64(value)] if value.is_finite())
        );
        assert_eq!(
            shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::Modularity), &output)
                .unwrap()
                .schema()
                .field(0)
                .is_nullable(),
            false
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            handler.execute(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            handler.execute(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        output_rows: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                )
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn conductance_handler_propagates_zero_volume_cancellation_and_limits() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[]);
        let partitions = conductance_partitions(&graph, &[(0, "alpha"), (1, "beta")]);
        let handler = Conductance {
            partitions: partitions.clone(),
        };
        assert_eq!(
            handler.execute(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
            ),
            Err(AlgorithmError::UndefinedConductance {
                partition: "alpha".into(),
            })
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            Conductance {
                partitions: partitions.clone(),
            }
            .execute(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );

        let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)]);
        assert!(matches!(
            Conductance {
                partitions: conductance_partitions(&graph, &[(0, "alpha"), (1, "beta")]),
            }
            .execute(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        output_rows: 1,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                ),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn conductance_rejects_directed_dispatch_before_storage_reads() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                None,
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::Conductance,
                    directed: true,
                    partition_property: Some("side".into()),
                    ..AnalyzeOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "conductance requires directed=false"
        ));
    }

    #[test]
    fn public_projection_fingerprint_is_stable_across_provider_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let options = AnalyzeOptions {
            by: AnalyzeAlgorithm::IsDag,
            directed: true,
            ..AnalyzeOptions::default()
        };
        let first_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let first = analyze_projection_fingerprint(
            &first_provider,
            dir.path(),
            OntologyMode::Strict,
            None,
            &options,
        )
        .unwrap();
        drop(first_provider);
        let reopened_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let reopened = analyze_projection_fingerprint(
            &reopened_provider,
            dir.path(),
            OntologyMode::Strict,
            None,
            &options,
        )
        .unwrap();
        assert_eq!(first, reopened);
        assert_ne!(first, [0; 32]);
    }

    #[test]
    fn modularity_rejects_directed_dispatch_before_storage_reads() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                None,
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::Modularity,
                    directed: true,
                    partition_property: Some("community".into()),
                    ..AnalyzeOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "modularity requires directed=false"
        ));
    }

    #[test]
    fn max_weight_matching_dispatches_canonical_weighted_uuid_rows() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            6,
            &[(9, 0, 1), (8, 0, 1), (7, 2, 3), (6, 4, 5), (10, 4, 4)],
        )
        .with_test_edge_weights(&[-1.0, -1.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 100.0]);
        let output = execute(
            &graph,
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MaxWeightMatching).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(u128::from(8_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
                    AlgorithmValue::Float64(5.0),
                ],
                vec![
                    AlgorithmValue::Uuid(u128::from(7_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                    AlgorithmValue::Float64(4.0),
                ],
            ]
        );
        let batch = shape_algorithm_output(
            Algorithm::Analyze(AnalyzeAlgorithm::MaxWeightMatching),
            &output,
        )
        .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", false),
                ("source_uuid", false),
                ("target_uuid", false),
                ("weight", true),
            ]
        );
    }

    #[test]
    fn max_cardinality_matching_dispatches_stable_unweighted_uuid_rows() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            6,
            &[
                (9, 0, 1),
                (8, 0, 1),
                (7, 1, 2),
                (6, 2, 0),
                (5, 1, 3),
                (4, 2, 4),
                (3, 5, 5),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::MaxCardinalityMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
                ],
                vec![
                    AlgorithmValue::Uuid(u128::from(5_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                ],
            ]
        );
        let batch = shape_algorithm_output(
            Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching),
            &output,
        )
        .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", false),
                ("source_uuid", false),
                ("target_uuid", false)
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "max_cardinality_matching"
        );
        for forbidden in [
            "weight",
            "confidence",
            "provenance_id",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        ] {
            assert!(batch.column_by_name(forbidden).is_none(), "{forbidden}");
        }
    }

    #[test]
    fn max_cardinality_matching_handles_empty_and_shared_controls() {
        let empty = execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::MaxCardinalityMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(empty.rows().is_empty());

        let graph = AdjacencyGraph::with_test_undirected_multigraph(4, &[(1, 0, 1), (2, 2, 3)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaxCardinalityMatching,
                false,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaxCardinalityMatching,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::MaxCardinalityMatching,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn max_cardinality_matching_rejects_directed_before_storage_reads() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                None,
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::MaxCardinalityMatching,
                    directed: true,
                    ..AnalyzeOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "max_cardinality_matching requires directed=false"
        ));
    }

    #[test]
    fn max_weight_matching_handles_empty_and_shared_controls() {
        let empty = execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(empty.rows().is_empty());

        let graph = AdjacencyGraph::with_test_undirected_multigraph(4, &[(1, 0, 1), (2, 2, 3)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaxWeightMatching,
                false,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaxWeightMatching,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::MaxWeightMatching,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn max_weight_matching_rejects_directed_and_nonfinite_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                None,
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::MaxWeightMatching,
                    directed: true,
                    ..AnalyzeOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "max_weight_matching requires directed=false"
        ));

        let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)])
            .with_test_edge_weights(&[f64::NAN, f64::NAN]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaxWeightMatching,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "weighted graph requires finite edge weights"
        ));
    }

    #[test]
    fn bipartite_matching_dispatches_stable_unweighted_uuid_rows() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            5,
            &[(9, 0, 3), (3, 0, 3), (4, 0, 4), (5, 1, 3), (6, 2, 4)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::MaxBipartiteMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                ],
                vec![
                    AlgorithmValue::Uuid(u128::from(6_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                    AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
                ],
            ]
        );
        let batch = shape_algorithm_output(
            Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching),
            &output,
        )
        .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", false),
                ("source_uuid", false),
                ("target_uuid", false)
            ]
        );
    }

    #[test]
    fn bipartite_matching_uses_explicit_partition_orientation() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(7, 0, 1)]);
        let partitions = ResolvedPartitionMap::try_new(
            graph.node_uuids(),
            [
                (
                    u128::from(0_u8).to_be_bytes(),
                    PartitionValue::String("z".into()),
                ),
                (
                    u128::from(1_u8).to_be_bytes(),
                    PartitionValue::String("a".into()),
                ),
            ],
        )
        .unwrap();
        let output = MaxBipartiteMatching {
            partitions: Some(partitions),
        }
        .execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        )
        .unwrap();
        assert_eq!(
            output.rows()[0][1..],
            [
                AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes())
            ]
        );
    }

    #[test]
    fn bipartite_matching_rejects_directed_dispatch_before_storage_reads() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                None,
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::MaxBipartiteMatching,
                    directed: true,
                    ..AnalyzeOptions::default()
                }
            ),
            Err(GfError::Validation(message))
                if message == "max_bipartite_matching requires directed=false"
        ));
    }

    #[test]
    fn bipartite_matching_handler_observes_pre_cancellation() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            MaxBipartiteMatching { partitions: None }.execute(
                &AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)]),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn is_dag_handles_empty_disconnected_and_parallel_graphs() {
        for graph in [
            AdjacencyGraph::default(),
            AdjacencyGraph::with_test_edges(6, &[(0, 1), (0, 1), (2, 3), (3, 4)]),
        ] {
            let output = execute(
                &graph,
                AnalyzeAlgorithm::IsDag,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(true)]]);
            assert_eq!(
                output.schema,
                Algorithm::Analyze(AnalyzeAlgorithm::IsDag).result_schema()
            );
        }
    }

    #[test]
    fn has_euler_circuit_shapes_boolean_for_empty_and_representative_graphs() {
        for (graph, directed, expected) in [
            (AdjacencyGraph::default(), false, true),
            (
                AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 0)]),
                false,
                true,
            ),
            (
                AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
                false,
                false,
            ),
            (
                AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
                true,
                true,
            ),
            (
                AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]),
                true,
                false,
            ),
        ] {
            let output = execute(
                &graph,
                AnalyzeAlgorithm::HasEulerCircuit,
                directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                output.schema,
                Algorithm::Analyze(AnalyzeAlgorithm::HasEulerCircuit).result_schema()
            );
            assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(expected)]]);
        }
    }

    #[test]
    fn has_euler_circuit_uses_shared_limits_and_cancellation() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::HasEulerCircuit,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::HasEulerCircuit,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn has_euler_path_shapes_boolean_for_empty_and_representative_graphs() {
        for (graph, directed, expected) in [
            (AdjacencyGraph::default(), false, true),
            (
                AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]),
                false,
                true,
            ),
            (
                AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 2), (0, 3)]),
                false,
                false,
            ),
            (
                AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 2)]),
                true,
                true,
            ),
            (
                AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (0, 2)]),
                true,
                false,
            ),
        ] {
            let output = execute(
                &graph,
                AnalyzeAlgorithm::HasEulerPath,
                directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                output.schema,
                Algorithm::Analyze(AnalyzeAlgorithm::HasEulerPath).result_schema()
            );
            assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(expected)]]);
        }
    }

    #[test]
    fn has_euler_path_uses_shared_limits_and_cancellation() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::HasEulerPath,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::HasEulerPath,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    fn euler_output(
        graph: &AdjacencyGraph,
        algorithm: AnalyzeAlgorithm,
        directed: bool,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute(
            graph,
            algorithm,
            directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
    }

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    #[test]
    fn euler_constructions_shape_empty_and_singleton_selections() {
        for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
            let empty = euler_output(&AdjacencyGraph::default(), algorithm, false).unwrap();
            assert_eq!(empty.schema, Algorithm::Analyze(algorithm).result_schema());
            assert!(empty.rows().is_empty());

            let singleton =
                euler_output(&AdjacencyGraph::with_test_edges(1, &[]), algorithm, false).unwrap();
            assert_eq!(
                singleton.rows(),
                [vec![
                    AlgorithmValue::UuidList(vec![uuid(0)]),
                    AlgorithmValue::UuidList(Vec::new()),
                ]]
            );
        }
    }

    #[test]
    fn euler_constructions_dispatch_directed_and_undirected_open_and_closed_trails() {
        let undirected_open =
            AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)]);
        assert_eq!(
            euler_output(&undirected_open, AnalyzeAlgorithm::EulerPath, false)
                .unwrap()
                .rows(),
            [vec![
                AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(2)]),
                AlgorithmValue::UuidList(vec![uuid(10), uuid(11)]),
            ]]
        );
        assert_eq!(
            euler_output(&undirected_open, AnalyzeAlgorithm::EulerCircuit, false),
            Err(AlgorithmError::UndefinedEulerCircuit)
        );

        let directed_open = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            euler_output(&directed_open, AnalyzeAlgorithm::EulerPath, true)
                .unwrap()
                .rows(),
            [vec![
                AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(2)]),
                AlgorithmValue::UuidList(vec![uuid(0), uuid(1)]),
            ]]
        );
        assert_eq!(
            euler_output(&directed_open, AnalyzeAlgorithm::EulerCircuit, true),
            Err(AlgorithmError::UndefinedEulerCircuit)
        );

        for (graph, directed) in [
            (
                AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 1), (11, 0, 1)]),
                false,
            ),
            (
                AdjacencyGraph::with_test_directed_edges(2, &[(0, 1), (1, 0)]),
                true,
            ),
        ] {
            let circuit = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, directed).unwrap();
            assert_eq!(circuit.rows().len(), 1);
            assert_eq!(
                circuit.rows()[0][0],
                AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(0)])
            );
            assert_eq!(
                circuit.rows()[0][1],
                AlgorithmValue::UuidList(if directed {
                    vec![uuid(0), uuid(1)]
                } else {
                    vec![uuid(10), uuid(11)]
                })
            );
        }
    }

    #[test]
    fn euler_constructions_preserve_loops_parallel_edges_and_structured_undefined_errors() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            2,
            &[(12, 0, 0), (10, 0, 1), (11, 0, 1)],
        );
        let first = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, false).unwrap();
        let second = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, false).unwrap();
        assert_eq!(first, second);
        let rows = first.rows();
        let [row] = rows.as_slice() else {
            panic!("Euler circuit must be one row");
        };
        let [
            AlgorithmValue::UuidList(nodes),
            AlgorithmValue::UuidList(edges),
        ] = row.as_slice()
        else {
            panic!("Euler row must contain UUID lists");
        };
        assert_eq!(nodes.len(), 4);
        assert_eq!(edges.len(), 3);
        assert!(
            edges.contains(&uuid(10)) && edges.contains(&uuid(11)) && edges.contains(&uuid(12))
        );

        let non_eulerian = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(10, 0, 1), (11, 0, 2), (12, 0, 3)],
        );
        assert_eq!(
            euler_output(&non_eulerian, AnalyzeAlgorithm::EulerPath, false),
            Err(AlgorithmError::UndefinedEulerPath)
        );
    }

    #[test]
    fn euler_constructions_are_repeatable_uuid_rename_equivariant_and_registered_once() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[uuid(90), uuid(20), uuid(70)],
            &[(0, 1), (1, 2)],
        );
        let output = euler_output(&graph, AnalyzeAlgorithm::EulerPath, true).unwrap();
        assert_eq!(
            output,
            euler_output(&graph, AnalyzeAlgorithm::EulerPath, true).unwrap()
        );
        assert_eq!(
            output.rows(),
            [vec![
                AlgorithmValue::UuidList(vec![uuid(90), uuid(20), uuid(70)]),
                AlgorithmValue::UuidList(vec![uuid(0), uuid(1)]),
            ]]
        );

        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, true).unwrap();
        for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
            assert_eq!(
                registry
                    .capabilities()
                    .iter()
                    .filter(|capability| capability.algorithm == Algorithm::Analyze(algorithm))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn euler_constructions_propagate_cancellation_and_resource_limits_atomically() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::EulerPath,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
        let run = |limits| {
            execute(
                &graph,
                AnalyzeAlgorithm::EulerPath,
                true,
                limits,
                AlgorithmCancellation::default(),
            )
        };
        assert!(matches!(
            run(AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            }),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            run(AlgorithmLimits {
                edges: 1,
                ..AlgorithmLimits::default()
            }),
            Err(AlgorithmError::EdgeLimit { .. })
        ));
        assert!(matches!(
            run(AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            }),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            run(AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            }),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn edge_coloring_dispatches_uuid_ordered_parallel_edge_colors() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            4,
            &[(14, 0, 2), (10, 0, 1), (12, 1, 2), (11, 0, 1), (20, 2, 3)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::EdgeColoring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::EdgeColoring).result_schema()
        );
        assert_eq!(
            output.rows(),
            [(10_u64, 0_u64), (11, 1), (12, 2), (14, 3), (20, 0),]
                .into_iter()
                .map(|(edge, color)| vec![
                    AlgorithmValue::Uuid(u128::from(edge).to_be_bytes()),
                    AlgorithmValue::UInt64(color),
                ])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn edge_coloring_handles_empty_loops_and_shared_controls() {
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::EdgeColoring,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            Vec::<Vec<AlgorithmValue>>::new()
        );
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_undirected_multigraph(1, &[(10, 0, 0)]),
                AnalyzeAlgorithm::EdgeColoring,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "edge_coloring cannot color a graph containing a self-loop"
        ));
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 1)]),
                AnalyzeAlgorithm::EdgeColoring,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::EdgeColoring,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn chromatic_number_dispatches_exact_scalar_for_representative_graphs() {
        for (graph, expected) in [
            (AdjacencyGraph::default(), 0),
            (AdjacencyGraph::with_test_edges(3, &[]), 1),
            (
                AdjacencyGraph::with_test_edges(5, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)]),
                3,
            ),
            (
                AdjacencyGraph::with_test_edges(
                    4,
                    &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)],
                ),
                4,
            ),
        ] {
            let output = execute(
                &graph,
                AnalyzeAlgorithm::ChromaticNumber,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                output.schema,
                Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber).result_schema()
            );
            assert_eq!(output.rows(), [vec![AlgorithmValue::UInt64(expected)]]);
        }
    }

    #[test]
    fn chromatic_number_preserves_loop_failure_and_shared_controls() {
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_edges(1, &[(0, 0)]),
                AnalyzeAlgorithm::ChromaticNumber,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message.contains("undefined for a graph containing a self-loop")
        ));
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
                AnalyzeAlgorithm::ChromaticNumber,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::ChromaticNumber,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn topological_sort_shapes_stable_uuid_order_and_positions() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16], [60; 16]],
            &[(0, 4), (0, 4), (1, 4), (2, 5), (3, 5)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::TopologicalSort,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::TopologicalSort).result_schema()
        );
        assert_eq!(
            output.rows(),
            [1_u64, 3, 2, 0, 4, 5]
                .into_iter()
                .enumerate()
                .map(|(order, node)| vec![
                    AlgorithmValue::Uuid(graph.node_uuid(node).unwrap()),
                    AlgorithmValue::UInt64(u64::try_from(order).unwrap()),
                ])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn dag_longest_path_dispatches_exact_deterministic_cost_and_path() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16], [60; 16]],
            &[(1, 3), (3, 0), (1, 2), (2, 0), (4, 5)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::DagLongestPath,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPath).result_schema()
        );
        assert_eq!(
            output.rows(),
            [vec![
                AlgorithmValue::Float64(2.0),
                AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [40; 16]]),
            ]]
        );
    }

    #[test]
    fn dag_longest_path_handles_empty_cycles_and_shared_controls() {
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPath,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            [vec![
                AlgorithmValue::Float64(0.0),
                AlgorithmValue::UuidList(Vec::new()),
            ]]
        );
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
                AnalyzeAlgorithm::DagLongestPath,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "dag_longest_path requires a directed acyclic graph"
        ));
        assert!(matches!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPath,
                true,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPath,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn weighted_dag_longest_path_dispatches_signed_cost_and_uuid_path() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16]],
            &[(1, 3), (3, 0), (1, 2), (2, 0), (4, 0)],
        )
        .with_test_edge_weights(&[2.0, 3.0, 2.0, 3.0, -8.0]);
        let output = execute(
            &graph,
            AnalyzeAlgorithm::DagLongestPathWeighted,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPathWeighted).result_schema()
        );
        assert_eq!(
            output.rows(),
            [vec![
                AlgorithmValue::Float64(5.0),
                AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [40; 16]]),
            ]]
        );
    }

    #[test]
    fn weighted_dag_longest_path_handles_empty_cycle_and_controls() {
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPathWeighted,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            [vec![
                AlgorithmValue::Float64(0.0),
                AlgorithmValue::UuidList(Vec::new()),
            ]]
        );
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_directed_edges(2, &[(0, 1), (1, 0)])
                    .with_test_edge_weights(&[1.0, 1.0]),
                AnalyzeAlgorithm::DagLongestPathWeighted,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "dag_longest_path_weighted requires a directed acyclic graph"
        ));
        assert!(matches!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPathWeighted,
                true,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::DagLongestPathWeighted,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn topological_sort_cycles_and_shared_controls_are_structured() {
        for graph in [
            AdjacencyGraph::with_test_directed_edges(1, &[(0, 0)]),
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
        ] {
            assert_eq!(
                execute(
                    &graph,
                    AnalyzeAlgorithm::TopologicalSort,
                    true,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap_err(),
                AlgorithmError::Execution {
                    message: "selected graph contains a cycle".into()
                }
            );
        }

        let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::TopologicalSort,
                true,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::TriangleCount,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::TopologicalSort,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn is_dag_rejects_directed_cycles_and_undirected_interpretation() {
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 0)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 0)]),
        ] {
            assert_eq!(
                execute(
                    &graph,
                    AnalyzeAlgorithm::IsDag,
                    true,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
                .rows(),
                [vec![AlgorithmValue::Boolean(false)]]
            );
        }
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::IsDag,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            [vec![AlgorithmValue::Boolean(false)]]
        );
    }

    #[test]
    fn is_dag_uses_shared_limits_cancellation_and_rust_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsDag,
                true,
                AlgorithmLimits {
                    nodes: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsDag,
                true,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsDag,
                true,
                AlgorithmLimits::default(),
                cancellation
            ),
            Err(AlgorithmError::Cancelled)
        );

        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, true).unwrap();
        assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
        assert!(matches!(
            registry.execute(
                Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default()
                ),
            ),
            Err(AlgorithmError::Unavailable { .. })
        ));
    }

    #[test]
    fn minimum_spanning_tree_shapes_uuid_forest_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_edges(
            6,
            &[(0, 1), (1, 0), (0, 2), (1, 2), (1, 3), (4, 5), (4, 4)],
        )
        .with_test_edge_weights(&[4.0, 4.0, 3.0, 1.0, 2.0, -2.0, -10.0]);
        let output = execute(
            &graph,
            AnalyzeAlgorithm::MinimumSpanningTree,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumSpanningTree).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                    AlgorithmValue::Float64(-2.0),
                ],
                vec![
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                    AlgorithmValue::Float64(2.0),
                ],
                vec![
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(3.0),
                ],
            ]
        );

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MinimumSpanningTree,
                false,
                AlgorithmLimits {
                    output_rows: 3,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        for limits in [
            AlgorithmLimits {
                nodes: 5,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                edges: 6,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(matches!(
                execute(
                    &graph,
                    AnalyzeAlgorithm::MinimumSpanningTree,
                    false,
                    limits,
                    AlgorithmCancellation::default(),
                ),
                Err(AlgorithmError::NodeLimit { .. } | AlgorithmError::EdgeLimit { .. })
            ));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::MinimumSpanningTree,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn minimum_k_spanning_tree_dispatches_canonical_ranked_rows() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            3,
            &[(10, 0, 1), (11, 0, 1), (12, 1, 2), (13, 0, 2), (14, 2, 2)],
        )
        .with_test_edge_weights(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 0.0]);
        let output = execute_minimum_k_spanning_tree(
            &graph,
            3,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::UInt64(0),
                    AlgorithmValue::Uuid(10_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::UInt64(0),
                    AlgorithmValue::Uuid(12_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::UInt64(1),
                    AlgorithmValue::Uuid(11_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::UInt64(1),
                    AlgorithmValue::Uuid(12_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::UInt64(2),
                    AlgorithmValue::Uuid(10_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Float64(1.0),
                ],
                vec![
                    AlgorithmValue::UInt64(2),
                    AlgorithmValue::Uuid(13_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(2.0),
                ],
            ]
        );
        assert_eq!(
            output,
            execute_minimum_k_spanning_tree(
                &graph,
                3,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );

        let batch = shape_algorithm_output(
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
            &output,
        )
        .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("tree_id", false),
                ("edge_uuid", false),
                ("source_uuid", false),
                ("target_uuid", false),
                ("weight", false),
            ]
        );
    }

    #[test]
    fn minimum_k_spanning_tree_dispatch_preserves_boundaries_and_controls() {
        for graph in [
            AdjacencyGraph::default(),
            AdjacencyGraph::with_test_edges(1, &[]),
        ] {
            assert!(
                execute_minimum_k_spanning_tree(
                    &graph,
                    1,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
                .rows()
                .is_empty()
            );
        }

        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (0, 2)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_minimum_k_spanning_tree(&graph, 2, AlgorithmLimits::default(), cancellation,),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_minimum_k_spanning_tree(
                &graph,
                2,
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
    fn maximum_spanning_tree_shapes_descending_uuid_forest_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 0), (0, 2), (1, 2), (1, 3), (4, 5), (4, 4)],
        )
        .with_test_edge_weights(&[4.0, 4.0, 3.0, 1.0, 2.0, -2.0, f64::MAX]);
        let output = execute(
            &graph,
            AnalyzeAlgorithm::MaximumSpanningTree,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::MaximumSpanningTree).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Float64(4.0),
                ],
                vec![
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::Float64(3.0),
                ],
                vec![
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                    AlgorithmValue::Float64(2.0),
                ],
                vec![
                    AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                    AlgorithmValue::Float64(-2.0),
                ],
            ]
        );

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaximumSpanningTree,
                false,
                AlgorithmLimits {
                    output_rows: 3,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::MaximumSpanningTree,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn find_cycles_dispatches_canonical_directed_and_undirected_uuid_lists() {
        let directed = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[[10; 16], [20; 16], [30; 16], [40; 16], [50; 16]],
            &[(0, 1), (1, 2), (2, 0), (1, 3), (3, 1), (3, 3), (4, 0)],
        );
        let output = execute(
            &directed,
            AnalyzeAlgorithm::FindCycles,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::FindCycles).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [30; 16]])],
                vec![AlgorithmValue::UuidList(vec![[20; 16], [40; 16]])],
                vec![AlgorithmValue::UuidList(vec![[40; 16]])],
            ]
        );

        let undirected = AdjacencyGraph::with_test_undirected_multigraph(
            5,
            &[
                (10, 0, 1),
                (11, 0, 1),
                (12, 1, 0),
                (13, 1, 2),
                (14, 2, 0),
                (15, 2, 3),
                (16, 3, 0),
                (17, 4, 4),
            ],
        );
        assert_eq!(
            execute(
                &undirected,
                AnalyzeAlgorithm::FindCycles,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            [
                vec![AlgorithmValue::UuidList(
                    [0_u128, 1, 2].map(u128::to_be_bytes).to_vec()
                )],
                vec![AlgorithmValue::UuidList(
                    [0_u128, 1, 2, 3].map(u128::to_be_bytes).to_vec()
                )],
                vec![AlgorithmValue::UuidList(
                    [0_u128, 2, 3].map(u128::to_be_bytes).to_vec()
                )],
                vec![AlgorithmValue::UuidList(vec![4_u128.to_be_bytes()])],
            ]
        );
    }

    #[test]
    fn find_cycles_dispatch_preserves_empty_and_shared_controls() {
        assert_eq!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::FindCycles,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            Vec::<Vec<AlgorithmValue>>::new()
        );
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::FindCycles,
                true,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::FindCycles,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn triangle_count_dispatches_exact_scalar_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            6,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 0, 1),
                (14, 1, 0),
                (15, 1, 3),
                (16, 2, 3),
                (17, 4, 4),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::TriangleCount,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::TriangleCount).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::UInt64(2)]]);

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::TriangleCount,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::TriangleCount,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn triad_census_dispatches_canonical_rows_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        let output = execute(
            &graph,
            AnalyzeAlgorithm::TriadCensus,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::TriadCensus).result_schema()
        );
        assert_eq!(output.rows().len(), 16);
        for (index, name) in TRIAD_NAMES.iter().enumerate() {
            assert_eq!(
                output.rows()[index],
                [
                    AlgorithmValue::Utf8((*name).to_owned()),
                    AlgorithmValue::UInt64(u64::from(index == 9)),
                ]
            );
        }

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::TriadCensus,
                true,
                AlgorithmLimits {
                    output_rows: 15,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::TriadCensus,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn transitivity_dispatches_exact_scalar_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            6,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 0, 1),
                (14, 1, 0),
                (15, 1, 3),
                (16, 2, 3),
                (17, 4, 4),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::Transitivity,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::Transitivity).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::Float64(0.75)]]);

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::Transitivity,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::Transitivity,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::Transitivity,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn is_planar_dispatches_exact_boolean_schema_and_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            6,
            &[
                (10, 0, 3),
                (11, 0, 4),
                (12, 0, 5),
                (13, 1, 3),
                (14, 1, 4),
                (15, 1, 5),
                (16, 2, 3),
                (17, 2, 4),
                (18, 2, 5),
                (19, 0, 3),
                (20, 0, 0),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::IsPlanar,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::IsPlanar).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(false)]]);

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsPlanar,
                false,
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsPlanar,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn dyad_census_dispatches_fixed_order_counts_and_shared_controls() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            5,
            &[(0, 1), (1, 0), (0, 1), (0, 2), (3, 2), (4, 4)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::DyadCensus,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Utf8("mutual".into()),
                    AlgorithmValue::UInt64(1),
                ],
                vec![
                    AlgorithmValue::Utf8("asymmetric".into()),
                    AlgorithmValue::UInt64(2),
                ],
                vec![
                    AlgorithmValue::Utf8("null".into()),
                    AlgorithmValue::UInt64(7),
                ],
            ]
        );

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::DyadCensus,
                true,
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
            execute(
                &graph,
                AnalyzeAlgorithm::DyadCensus,
                true,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn dyad_census_shapes_canonical_arrow_schema_and_metadata() {
        let output = execute(
            &AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]),
            AnalyzeAlgorithm::DyadCensus,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let algorithm = Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus);
        let batch = shape_algorithm_output(algorithm, &output).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "dyad_type");
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.schema().field(1).name(), "count");
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(
            batch.schema().metadata().get("graphforge.algorithm"),
            Some(&"dyad_census".to_owned())
        );
        assert_eq!(
            batch.schema().metadata().get("graphforge.verb"),
            Some(&"analyze".to_owned())
        );
        assert_eq!(
            batch
                .schema()
                .metadata()
                .get("graphforge.algorithm_schema_version"),
            Some(&"1".to_owned())
        );
    }

    #[test]
    fn node_coloring_dispatches_uuid_colors_with_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            5,
            &[
                (10, 0, 1),
                (11, 0, 2),
                (12, 1, 2),
                (13, 2, 3),
                (14, 0, 1),
                (15, 1, 0),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::NodeColoring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                    AlgorithmValue::UInt64(0),
                ],
                vec![
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::UInt64(1),
                ],
                vec![
                    AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                    AlgorithmValue::UInt64(2),
                ],
                vec![
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                    AlgorithmValue::UInt64(0),
                ],
                vec![
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                    AlgorithmValue::UInt64(0),
                ],
            ]
        );

        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::NodeColoring,
                false,
                AlgorithmLimits {
                    output_rows: 4,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::NodeColoring,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::NodeColoring,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn k1_coloring_dispatches_dedicated_uuid_ordered_rows_and_schema() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            5,
            &[(10, 0, 3), (11, 1, 2), (12, 2, 3), (13, 3, 2), (14, 0, 3)],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring).result_schema()
        );
        assert_eq!(
            output.rows(),
            [0_u64, 1, 0, 1, 0]
                .into_iter()
                .enumerate()
                .map(|(node, color)| vec![
                    AlgorithmValue::Uuid(u128::try_from(node).unwrap().to_be_bytes()),
                    AlgorithmValue::UInt64(color),
                ])
                .collect::<Vec<_>>()
        );

        let legacy = execute(
            &graph,
            AnalyzeAlgorithm::NodeColoring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_ne!(output.rows(), legacy.rows());
        assert_ne!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber).result_schema()
        );

        let batch =
            shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring), &output)
                .unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.schema().field(0).name(), "node_uuid");
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.schema().field(1).name(), "color");
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(
            batch.schema().metadata().get("graphforge.algorithm"),
            Some(&"k1_coloring".to_owned())
        );
        assert_eq!(
            batch
                .schema()
                .metadata()
                .get("graphforge.algorithm_schema_version"),
            Some(&"1".to_owned())
        );
    }

    #[test]
    fn k1_coloring_handler_preserves_empty_loop_cancellation_and_limits() {
        assert!(
            execute(
                &AdjacencyGraph::default(),
                AnalyzeAlgorithm::K1Coloring,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_undirected_multigraph(1, &[(10, 0, 0)]),
                AnalyzeAlgorithm::K1Coloring,
                false,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::Execution { message })
                if message == "k1_coloring cannot color a graph containing a self-loop"
        ));
        let graph = AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 0, 1)]);
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::K1Coloring,
                false,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::K1Coloring,
                false,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::K1Coloring,
                false,
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
            execute(
                &graph,
                AnalyzeAlgorithm::K1Coloring,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn k1_coloring_rejects_directed_weight_and_unrelated_options() {
        let dir = tempfile::tempdir().unwrap();
        let provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        for (options, expected) in [
            (
                AnalyzeOptions {
                    by: AnalyzeAlgorithm::K1Coloring,
                    directed: true,
                    ..AnalyzeOptions::default()
                },
                "k1_coloring requires directed=false",
            ),
            (
                AnalyzeOptions {
                    by: AnalyzeAlgorithm::K1Coloring,
                    directed: false,
                    weight: Some("cost".into()),
                    ..AnalyzeOptions::default()
                },
                "k1_coloring does not accept an edge weight property",
            ),
        ] {
            assert!(matches!(
                analyze_algorithm(
                    &provider,
                    dir.path(),
                    OntologyMode::Strict,
                    None,
                    &options
                ),
                Err(GfError::Validation(message)) if message == expected
            ));
        }
        for options in [
            AnalyzeOptions {
                by: AnalyzeAlgorithm::K1Coloring,
                directed: false,
                k: Some(2),
                ..AnalyzeOptions::default()
            },
            AnalyzeOptions {
                by: AnalyzeAlgorithm::K1Coloring,
                directed: false,
                partition_property: Some("partition".into()),
                ..AnalyzeOptions::default()
            },
        ] {
            assert!(matches!(
                normalize_analyze_options(&options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn articulation_points_dispatches_uuid_rows_with_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            8,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 1, 3),
                (14, 3, 1),
                (15, 3, 4),
                (16, 3, 3),
                (17, 5, 6),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::ArticulationPoints,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::ArticulationPoints).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![AlgorithmValue::Uuid(1_u128.to_be_bytes())],
                vec![AlgorithmValue::Uuid(3_u128.to_be_bytes())],
            ]
        );

        for limits in [
            AlgorithmLimits {
                nodes: 7,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                edges: 14,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(matches!(
                execute(
                    &graph,
                    AnalyzeAlgorithm::ArticulationPoints,
                    false,
                    limits,
                    AlgorithmCancellation::default(),
                ),
                Err(AlgorithmError::NodeLimit { .. }
                    | AlgorithmError::EdgeLimit { .. }
                    | AlgorithmError::OutputLimit { .. })
            ));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::ArticulationPoints,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn articulation_points_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            10,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 1, 3),
                (14, 3, 4),
                (15, 4, 5),
                (16, 5, 3),
                (17, 5, 6),
                (18, 6, 7),
                (19, 7, 5),
                (20, 7, 8),
                (21, 8, 9),
                (22, 7, 8),
            ],
        );
        let serial =
            execute_with_compute_threads(&graph, AnalyzeAlgorithm::ArticulationPoints, false, 1)
                .unwrap();
        assert_eq!(
            serial.rows(),
            [
                vec![AlgorithmValue::Uuid(1_u128.to_be_bytes())],
                vec![AlgorithmValue::Uuid(3_u128.to_be_bytes())],
                vec![AlgorithmValue::Uuid(5_u128.to_be_bytes())],
                vec![AlgorithmValue::Uuid(7_u128.to_be_bytes())],
                vec![AlgorithmValue::Uuid(8_u128.to_be_bytes())],
            ]
        );
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output = execute_with_compute_threads(
                &graph,
                AnalyzeAlgorithm::ArticulationPoints,
                false,
                threads,
            )
            .unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }

    #[test]
    fn bridges_dispatches_canonical_uuid_rows_with_shared_controls() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            8,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 1, 3),
                (14, 3, 1),
                (15, 3, 4),
                (16, 3, 3),
                (17, 5, 6),
            ],
        );
        let output = execute(
            &graph,
            AnalyzeAlgorithm::Bridges,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::Bridges).result_schema()
        );
        assert_eq!(
            output.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(15_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                ],
                vec![
                    AlgorithmValue::Uuid(17_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(6_u128.to_be_bytes()),
                ],
            ]
        );

        for limits in [
            AlgorithmLimits {
                nodes: 7,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                edges: 14,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(matches!(
                execute(
                    &graph,
                    AnalyzeAlgorithm::Bridges,
                    false,
                    limits,
                    AlgorithmCancellation::default(),
                ),
                Err(AlgorithmError::NodeLimit { .. }
                    | AlgorithmError::EdgeLimit { .. }
                    | AlgorithmError::OutputLimit { .. })
            ));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::Bridges,
                false,
                AlgorithmLimits::default(),
                cancellation,
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn bridges_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            10,
            &[
                (10, 0, 1),
                (11, 1, 2),
                (12, 2, 0),
                (13, 1, 3),
                (14, 3, 4),
                (15, 4, 5),
                (16, 5, 3),
                (17, 5, 6),
                (18, 6, 7),
                (19, 7, 5),
                (20, 7, 8),
                (21, 8, 9),
                (22, 7, 8),
            ],
        );
        let serial =
            execute_with_compute_threads(&graph, AnalyzeAlgorithm::Bridges, false, 1).unwrap();
        assert_eq!(
            serial.rows(),
            [
                vec![
                    AlgorithmValue::Uuid(13_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                ],
                vec![
                    AlgorithmValue::Uuid(21_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(8_u128.to_be_bytes()),
                    AlgorithmValue::Uuid(9_u128.to_be_bytes()),
                ],
            ]
        );
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output =
                execute_with_compute_threads(&graph, AnalyzeAlgorithm::Bridges, false, threads)
                    .unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }

    #[test]
    fn graphsage_source_resource_contract_validates_feature_matrix() {
        let empty = AdjacencyGraph::with_test_counts(0, 0);
        assert!(graphsage_source_resources(&empty).is_err());

        let mut graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        assert_eq!(
            graphsage_source_resources(&graph).unwrap_err().to_string(),
            "validation error: graphsage selected node has no resolved feature vector"
        );
        graph
            .replace_node_vectors(HashMap::from([(0, vec![]), (1, vec![])]))
            .unwrap();
        assert!(
            graphsage_source_resources(&graph)
                .unwrap_err()
                .to_string()
                .contains("non-empty")
        );
        graph
            .replace_node_vectors(HashMap::from([(0, vec![1.0, 2.0]), (1, vec![3.0])]))
            .unwrap();
        assert!(
            graphsage_source_resources(&graph)
                .unwrap_err()
                .to_string()
                .contains("inconsistent shape")
        );
        graph
            .replace_node_vectors(HashMap::from([
                (0, vec![1.0, 2.0]),
                (1, vec![f64::NAN, 4.0]),
            ]))
            .unwrap();
        assert!(
            graphsage_source_resources(&graph)
                .unwrap_err()
                .to_string()
                .contains("must be finite")
        );
        graph
            .replace_node_vectors(HashMap::from([(0, vec![1.0, 2.0]), (1, vec![3.0, 4.0])]))
            .unwrap();
        let (width, retained) = graphsage_source_resources(&graph).unwrap();
        assert_eq!(width, 2);
        assert!(retained >= 96);
        let projection = graphsage_projection(&graph).expect("valid GraphSAGE projection");
        assert_eq!(projection.nodes().len(), 2);
        assert_eq!(projection.feature_width(), 2);
    }
}
