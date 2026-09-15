//! Flow cut adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, Arc, BUILTIN_REVIEW, CostCapacityEdge, GfError, PathAlgorithm,
    PathsOptions, RustAlgorithm, capacity_edges, execution, gomory_hu_forest, invalid_selector,
    maximum_flow, min_cost_flow_adjacency_entries, minimum_cost_maximum_flow, minimum_cut,
    shape_min_cost_flow_output,
};

pub(super) struct MaxFlow {
    pub(super) source: [u8; 16],
    pub(super) target: Option<[u8; 16]>,
    pub(super) edges: bool,
}

pub(super) struct MinCut {
    pub(super) source: [u8; 16],
    pub(super) target: Option<[u8; 16]>,
    pub(super) edges: bool,
}

pub(super) struct GomoryHuTree;

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

pub(super) struct MinCostFlow {
    pub(super) source: [u8; 16],
    pub(super) target: Option<[u8; 16]>,
    pub(super) edges: bool,
    pub(super) input: Arc<[CostCapacityEdge]>,
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

pub(super) fn cost_capacity_edges(
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

pub(super) fn validate_gomory_hu_invocation(
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

pub(super) fn validate_min_cost_properties(
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

#[cfg(test)]
mod tests;
