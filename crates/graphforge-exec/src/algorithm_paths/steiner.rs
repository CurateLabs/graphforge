//! Steiner adapters and projection.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, Arc, BUILTIN_REVIEW, GfError, NodePrize,
    Path, PathAlgorithm, PathsOptions, PrizeSteinerInputEdge, RecordBatch, ResolvedNumber,
    RustAlgorithm, SteinerKind, WeightedEdge, capacity_edges, execution,
    load_node_numeric_property, minimum_steiner_tree, normalize_steiner_invocation,
    prize_collecting_steiner_tree, shape_algorithm_output,
};

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

pub(super) fn execute_steiner(
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

pub(super) fn validate_source_and_steiner_fields(
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

pub(super) const fn steiner_kind(by: PathAlgorithm) -> Option<SteinerKind> {
    match by {
        PathAlgorithm::MinSteinerTree => Some(SteinerKind::MinimumTree),
        PathAlgorithm::PrizeCollectingSteinerTree => Some(SteinerKind::PrizeCollecting),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
