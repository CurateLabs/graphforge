//! Rust-owned adapter from [`AdjacencyProvider`] to the graph consumed by M18.
//!
//! The provider remains the sole adjacency implementation. This module adds
//! stable UUID metadata, optional label selection, and optional edge weights;
//! it never constructs adjacency from Parquet topology rows.
#![allow(
    dead_code,
    reason = "M18 foundation consumed by the Rust dispatch slice in issue #1146"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use arrow::array::{
    Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use graphforge_core::{GfError, OntologyMode, TypeId};
use graphforge_ir::{Direction, IrLiteral};
use sha2::{Digest, Sha256};

use crate::adjacency::AdjacencyProvider;
use crate::algorithm_partition::{PartitionValue, ResolvedPartitionMap};

type NodeUuidMap = HashMap<u64, [u8; 16]>;

const VECTOR_NODE_LIMIT: usize = 4_096;
const VECTOR_CELL_LIMIT: usize = 16_777_216;

/// Selection applied while exporting the shared adjacency view.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdjacencySelection<'a> {
    /// Resolved node label id. `None` includes every node.
    pub label: Option<TypeId>,
    /// Relationship name, or `"*"` for all relationships.
    pub via: &'a str,
    /// Traversal direction requested from the provider.
    pub direction: Direction,
    /// Edge property used as weight. `None` assigns `1.0` to every edge.
    pub weight: Option<&'a str>,
}

/// One algorithm-facing adjacency entry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AlgorithmEdge {
    /// Execution-internal edge surrogate.
    pub edge_id: u64,
    /// Stable public edge identity.
    pub edge_uuid: [u8; 16],
    /// Execution-internal neighbor surrogate.
    pub neighbor_id: u64,
    /// Selected weight, or the unweighted default `1.0`.
    pub weight: f64,
}

/// Knowledge-agnostic graph projection resolved before M18 dispatch.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ResolvedGraphProjection {
    pub(crate) directed: bool,
    pub(crate) nodes: Vec<[u8; 16]>,
    pub(crate) edges: Vec<ResolvedGraphEdge>,
}

/// One logical edge in a resolved graph projection.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ResolvedGraphEdge {
    pub(crate) edge_uuid: [u8; 16],
    pub(crate) source_uuid: [u8; 16],
    pub(crate) target_uuid: [u8; 16],
    pub(crate) weight: f64,
}

/// Versioned digest of the exact public-identity projection consumed by M18.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AlgorithmProjectionFingerprint([u8; 32]);

impl AlgorithmProjectionFingerprint {
    /// Full canonical projection digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LogicalProjectionEdge {
    source_uuid: [u8; 16],
    target_uuid: [u8; 16],
    weight_bits: u64,
    mirror_count: u8,
}

/// Algorithm graph with deterministic surrogate ordering and UUID round trips.
#[derive(Clone, Debug, Default)]
pub(crate) struct AdjacencyGraph {
    directed: bool,
    node_ids: Vec<u64>,
    node_uuid_by_id: HashMap<u64, [u8; 16]>,
    node_id_by_uuid: HashMap<[u8; 16], u64>,
    neighbors: HashMap<u64, Vec<AlgorithmEdge>>,
    node_vectors: HashMap<u64, Vec<f64>>,
}

impl AdjacencyGraph {
    /// Fingerprint public UUID topology without execution surrogates.
    pub(crate) fn projection_fingerprint(&self) -> Result<AlgorithmProjectionFingerprint, GfError> {
        let mut nodes = self.node_uuids().collect::<Vec<_>>();
        nodes.sort_unstable();
        let node_count = u64::try_from(nodes.len()).map_err(|_| {
            GfError::Execution("algorithm projection node count exceeds UInt64 range".into())
        })?;

        let mut logical_edges: BTreeMap<[u8; 16], LogicalProjectionEdge> = BTreeMap::new();
        for &source_id in &self.node_ids {
            let source_uuid = self.node_uuid(source_id).ok_or_else(|| {
                GfError::Execution("algorithm projection source has no UUID identity".into())
            })?;
            for edge in self.neighbors(source_id) {
                let target_uuid = self.node_uuid(edge.neighbor_id).ok_or_else(|| {
                    GfError::Execution("algorithm projection target has no UUID identity".into())
                })?;
                if !edge.weight.is_finite() {
                    return Err(GfError::Execution(
                        "algorithm projection edge weight is not finite".into(),
                    ));
                }
                let (source, target) = if self.directed || source_uuid <= target_uuid {
                    (source_uuid, target_uuid)
                } else {
                    (target_uuid, source_uuid)
                };
                let weight = edge.weight.to_bits();
                match logical_edges.entry(edge.edge_uuid) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(LogicalProjectionEdge {
                            source_uuid: source,
                            target_uuid: target,
                            weight_bits: weight,
                            mirror_count: 1,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let existing = entry.get_mut();
                        if self.directed
                            || existing.source_uuid != source
                            || existing.target_uuid != target
                            || existing.weight_bits != weight
                            || existing.mirror_count == 2
                        {
                            return Err(GfError::Execution(format!(
                                "algorithm projection edge {} has inconsistent adjacency identity",
                                uuid_text(&edge.edge_uuid)
                            )));
                        }
                        existing.mirror_count = 2;
                    }
                }
            }
        }
        if !self.directed && logical_edges.values().any(|edge| edge.mirror_count != 2) {
            return Err(GfError::Execution(
                "undirected algorithm projection edge is missing its mirrored adjacency".into(),
            ));
        }
        let edge_count = u64::try_from(logical_edges.len()).map_err(|_| {
            GfError::Execution("algorithm projection edge count exceeds UInt64 range".into())
        })?;

        let mut digest = Sha256::new();
        digest.update(b"graphforge_algorithm_projection_v1");
        digest.update([u8::from(self.directed)]);
        digest.update(node_count.to_le_bytes());
        for uuid in nodes {
            digest.update(uuid);
        }
        digest.update(edge_count.to_le_bytes());
        for (edge_uuid, edge) in logical_edges {
            digest.update(edge_uuid);
            digest.update(edge.source_uuid);
            digest.update(edge.target_uuid);
            digest.update(edge.weight_bits.to_le_bytes());
        }
        Ok(AlgorithmProjectionFingerprint(digest.finalize().into()))
    }

    /// Fingerprint topology plus graph-native vectors loaded for one descriptor.
    pub(crate) fn descriptor_projection_fingerprint(
        &self,
    ) -> Result<AlgorithmProjectionFingerprint, GfError> {
        let topology = self.projection_fingerprint()?;
        let mut vectors = self
            .node_vectors
            .iter()
            .map(|(node_id, vector)| {
                self.node_uuid(*node_id)
                    .map(|uuid| (uuid, vector))
                    .ok_or_else(|| {
                        GfError::Execution(
                            "algorithm vector projection has no UUID identity".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        vectors.sort_unstable_by_key(|(uuid, _)| *uuid);
        let mut digest = Sha256::new();
        digest.update(b"graphforge_algorithm_descriptor_projection_v1");
        digest.update(topology.as_bytes());
        digest.update(
            u64::try_from(vectors.len())
                .map_err(|_| {
                    GfError::Execution(
                        "algorithm vector projection count exceeds UInt64 range".into(),
                    )
                })?
                .to_be_bytes(),
        );
        for (uuid, vector) in vectors {
            digest.update(uuid);
            digest.update(
                u64::try_from(vector.len())
                    .map_err(|_| {
                        GfError::Execution("algorithm vector dimension exceeds UInt64 range".into())
                    })?
                    .to_be_bytes(),
            );
            for value in vector {
                if !value.is_finite() {
                    return Err(GfError::Execution(
                        "algorithm vector projection contains a non-finite value".into(),
                    ));
                }
                digest.update(value.to_bits().to_be_bytes());
            }
        }
        Ok(AlgorithmProjectionFingerprint(digest.finalize().into()))
    }

    /// Validate and canonicalize an explicitly resolved UUID projection.
    pub(crate) fn from_resolved_projection(
        mut projection: ResolvedGraphProjection,
    ) -> Result<Self, GfError> {
        projection.nodes.sort_unstable();
        if projection.nodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(GfError::Validation(
                "resolved graph projection contains duplicate node UUID".into(),
            ));
        }
        let node_ids = (0..projection.nodes.len())
            .map(|index| {
                u64::try_from(index).map_err(|_| {
                    GfError::Validation(
                        "resolved graph projection node count exceeds UInt64 range".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let node_uuid_by_id = node_ids
            .iter()
            .copied()
            .zip(projection.nodes.iter().copied())
            .collect::<HashMap<_, _>>();
        let node_id_by_uuid = node_uuid_by_id
            .iter()
            .map(|(&id, &uuid)| (uuid, id))
            .collect::<HashMap<_, _>>();

        projection
            .edges
            .sort_unstable_by_key(|edge| (edge.edge_uuid, edge.source_uuid, edge.target_uuid));
        if projection
            .edges
            .windows(2)
            .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
        {
            return Err(GfError::Validation(
                "resolved graph projection contains duplicate edge UUID".into(),
            ));
        }
        let mut neighbors: HashMap<u64, Vec<AlgorithmEdge>> = HashMap::new();
        for (index, edge) in projection.edges.into_iter().enumerate() {
            if !edge.weight.is_finite() {
                return Err(GfError::Validation(
                    "resolved graph projection edge weight must be finite".into(),
                ));
            }
            let source = node_id_by_uuid
                .get(&edge.source_uuid)
                .copied()
                .ok_or_else(|| {
                    GfError::Validation(
                        "resolved graph projection edge source UUID is not selected".into(),
                    )
                })?;
            let target = node_id_by_uuid
                .get(&edge.target_uuid)
                .copied()
                .ok_or_else(|| {
                    GfError::Validation(
                        "resolved graph projection edge target UUID is not selected".into(),
                    )
                })?;
            let edge_id = u64::try_from(index).map_err(|_| {
                GfError::Validation(
                    "resolved graph projection edge count exceeds UInt64 range".into(),
                )
            })?;
            neighbors.entry(source).or_default().push(AlgorithmEdge {
                edge_id,
                edge_uuid: edge.edge_uuid,
                neighbor_id: target,
                weight: edge.weight,
            });
            if !projection.directed {
                neighbors.entry(target).or_default().push(AlgorithmEdge {
                    edge_id,
                    edge_uuid: edge.edge_uuid,
                    neighbor_id: source,
                    weight: edge.weight,
                });
            }
        }
        for entries in neighbors.values_mut() {
            entries.sort_by_key(|edge| (edge.edge_id, edge.neighbor_id));
        }
        Ok(Self {
            directed: projection.directed,
            node_ids,
            node_uuid_by_id,
            node_id_by_uuid,
            neighbors,
            node_vectors: HashMap::new(),
        })
    }

    /// Whether the exported topology preserves edge orientation.
    #[must_use]
    pub(crate) fn is_directed(&self) -> bool {
        self.directed
    }

    /// Selected public node identities in deterministic surrogate order.
    pub(crate) fn node_uuids(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.node_ids
            .iter()
            .map(|node_id| self.node_uuid_by_id[node_id])
    }

    /// Selected node surrogates in ascending order.
    #[must_use]
    pub(crate) fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    /// Adjacency entries for `node_id`, ordered by `(edge_id, neighbor_id)`.
    #[must_use]
    pub(crate) fn neighbors(&self, node_id: u64) -> &[AlgorithmEdge] {
        self.neighbors.get(&node_id).map_or(&[], Vec::as_slice)
    }

    /// Resolve an internal node surrogate to its public UUID.
    #[must_use]
    pub(crate) fn node_uuid(&self, node_id: u64) -> Option<[u8; 16]> {
        self.node_uuid_by_id.get(&node_id).copied()
    }

    /// Resolve a public UUID to its internal node surrogate.
    #[must_use]
    pub(crate) fn node_id(&self, node_uuid: &[u8; 16]) -> Option<u64> {
        self.node_id_by_uuid.get(node_uuid).copied()
    }

    /// Validated feature vector for one selected node.
    #[must_use]
    pub(crate) fn node_vector(&self, node_id: u64) -> Option<&[f64]> {
        self.node_vectors.get(&node_id).map(Vec::as_slice)
    }

    /// Replace the resolved feature matrix after property loading.
    pub(crate) fn replace_node_vectors(
        &mut self,
        vectors: HashMap<u64, Vec<f64>>,
    ) -> Result<(), GfError> {
        if vectors.len() != self.node_ids.len()
            || self
                .node_ids
                .iter()
                .any(|node_id| !vectors.contains_key(node_id))
        {
            return Err(GfError::Execution(
                "resolved feature matrix does not match the selected node projection".into(),
            ));
        }
        self.node_vectors = vectors;
        Ok(())
    }

    /// Whether no nodes survived selection.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }

    /// Number of direction-expanded adjacency entries.
    pub(crate) fn edge_entry_count(&self) -> u64 {
        self.neighbors
            .values()
            .map(|entries| u64::try_from(entries.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add)
    }

    #[cfg(test)]
    pub(crate) fn with_test_counts(nodes: u64, edges: u64) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let node_uuid_by_id = node_ids
            .iter()
            .map(|&id| (id, u128::from(id).to_be_bytes()))
            .collect::<HashMap<_, _>>();
        let node_id_by_uuid = node_uuid_by_id
            .iter()
            .map(|(&id, &uuid)| (uuid, id))
            .collect();
        let neighbors = if nodes == 0 {
            HashMap::new()
        } else {
            HashMap::from([(
                0,
                (0..edges)
                    .map(|edge_id| AlgorithmEdge {
                        edge_id,
                        edge_uuid: u128::from(edge_id).to_be_bytes(),
                        neighbor_id: edge_id % nodes,
                        weight: 1.0,
                    })
                    .collect(),
            )])
        };
        Self {
            directed: false,
            node_ids,
            node_uuid_by_id,
            node_id_by_uuid,
            neighbors,
            node_vectors: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_edges(nodes: u64, edges: &[(u64, u64)]) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let node_uuid_by_id = node_ids
            .iter()
            .map(|&id| (id, u128::from(id).to_be_bytes()))
            .collect::<HashMap<_, _>>();
        let node_id_by_uuid = node_uuid_by_id
            .iter()
            .map(|(&id, &uuid)| (uuid, id))
            .collect();
        let mut neighbors: HashMap<u64, Vec<AlgorithmEdge>> = HashMap::new();
        for (edge_id, &(source, target)) in edges.iter().enumerate() {
            let edge_id = u64::try_from(edge_id).expect("test edge count fits u64");
            neighbors.entry(source).or_default().push(AlgorithmEdge {
                edge_id,
                edge_uuid: u128::from(edge_id).to_be_bytes(),
                neighbor_id: target,
                weight: 1.0,
            });
        }
        Self {
            directed: false,
            node_ids,
            node_uuid_by_id,
            node_id_by_uuid,
            neighbors,
            node_vectors: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_directed_edges(nodes: u64, edges: &[(u64, u64)]) -> Self {
        let mut graph = Self::with_test_edges(nodes, edges);
        graph.directed = true;
        graph
    }

    #[cfg(test)]
    pub(crate) fn with_test_directed_edges_and_uuids(
        node_uuids: &[[u8; 16]],
        edges: &[(u64, u64)],
    ) -> Self {
        let mut graph =
            Self::with_test_directed_edges(u64::try_from(node_uuids.len()).unwrap(), edges);
        graph.node_uuid_by_id = node_uuids
            .iter()
            .copied()
            .enumerate()
            .map(|(id, uuid)| (u64::try_from(id).unwrap(), uuid))
            .collect();
        graph.node_id_by_uuid = graph
            .node_uuid_by_id
            .iter()
            .map(|(&id, &uuid)| (uuid, id))
            .collect();
        graph
    }

    #[cfg(test)]
    pub(crate) fn with_test_undirected_multigraph(nodes: u64, edges: &[(u64, u64, u64)]) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let node_uuid_by_id = node_ids
            .iter()
            .map(|&id| (id, u128::from(id).to_be_bytes()))
            .collect::<HashMap<_, _>>();
        let node_id_by_uuid = node_uuid_by_id
            .iter()
            .map(|(&id, &uuid)| (uuid, id))
            .collect();
        let mut neighbors: HashMap<u64, Vec<AlgorithmEdge>> = HashMap::new();
        for &(edge_id, source, target) in edges {
            let edge = |neighbor_id| AlgorithmEdge {
                edge_id,
                edge_uuid: u128::from(edge_id).to_be_bytes(),
                neighbor_id,
                weight: 1.0,
            };
            neighbors.entry(source).or_default().push(edge(target));
            if source != target {
                neighbors.entry(target).or_default().push(edge(source));
            }
        }
        Self {
            directed: false,
            node_ids,
            node_uuid_by_id,
            node_id_by_uuid,
            neighbors,
            node_vectors: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_edge_weights(mut self, weights: &[f64]) -> Self {
        let mut edges = self.neighbors.values_mut().flatten().collect::<Vec<_>>();
        edges.sort_unstable_by_key(|edge| edge.edge_id);
        assert_eq!(edges.len(), weights.len());
        for (edge, &weight) in edges.into_iter().zip(weights) {
            edge.weight = weight;
        }
        self
    }
}

/// Export only stable node selection and UUID identity, without reading edges.
pub(crate) fn export_node_selection(
    dir: &Path,
    label: Option<TypeId>,
) -> Result<AdjacencyGraph, GfError> {
    let (node_ids, node_uuid_by_id) = selected_nodes(dir, label)?;
    let node_id_by_uuid = node_uuid_by_id
        .iter()
        .map(|(&node_id, &uuid)| (uuid, node_id))
        .collect();
    Ok(AdjacencyGraph {
        directed: false,
        node_ids,
        node_uuid_by_id,
        node_id_by_uuid,
        neighbors: HashMap::new(),
        node_vectors: HashMap::new(),
    })
}

/// Export the provider's adjacency into the Rust algorithm graph.
///
/// A requested weight property is strict: every selected edge must have one
/// non-NULL numeric value. Missing, NULL, non-numeric, NaN, and infinite values
/// are validation errors. Negative finite weights are preserved so individual
/// algorithms can accept or reject them according to their contract.
///
/// # Errors
/// Returns storage/execution errors from the provider or metadata reads, and
/// [`GfError::Validation`] for invalid selected weights.
pub(crate) fn export_adjacency(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    selection: AdjacencySelection<'_>,
) -> Result<AdjacencyGraph, GfError> {
    let adjacency = provider.adjacency(selection.via, selection.direction)?;
    let (node_ids, node_uuid_by_id) = selected_nodes(dir, selection.label)?;
    let selected: HashSet<u64> = node_ids.iter().copied().collect();

    let mut raw = Vec::new();
    let mut edge_ids = HashSet::new();
    for (node_id, entries) in adjacency.rows() {
        if !selected.contains(&node_id) {
            continue;
        }
        for &(edge_id, neighbor_id) in entries {
            if selected.contains(&neighbor_id) {
                raw.push((node_id, edge_id, neighbor_id));
                edge_ids.insert(edge_id);
            }
        }
    }
    raw.sort_unstable();

    let edge_uuids = selected_edge_uuids(dir, mode, selection.via, &edge_ids)?;
    let weights = match selection.weight {
        Some(property) => selected_weights(dir, selection.via, property, &edge_uuids)?,
        None => HashMap::new(),
    };

    let mut neighbors: HashMap<u64, Vec<AlgorithmEdge>> = HashMap::new();
    for (node_id, edge_id, neighbor_id) in raw {
        let edge_uuid = edge_uuids.get(&edge_id).copied().ok_or_else(|| {
            GfError::Execution(format!("adjacency edge {edge_id} has no topology UUID"))
        })?;
        let weight = selection.weight.map_or(1.0, |_| {
            weights.get(&edge_uuid).copied().unwrap_or(f64::NAN)
        });
        if !weight.is_finite() {
            return Err(GfError::Validation(format!(
                "edge weight is missing, NULL, NaN, or infinite for edge {}",
                uuid_text(&edge_uuid)
            )));
        }
        neighbors.entry(node_id).or_default().push(AlgorithmEdge {
            edge_id,
            edge_uuid,
            neighbor_id,
            weight,
        });
    }
    for entries in neighbors.values_mut() {
        entries.sort_by_key(|edge| (edge.edge_id, edge.neighbor_id));
    }
    let node_id_by_uuid = node_uuid_by_id
        .iter()
        .map(|(&id, &uuid)| (uuid, id))
        .collect();
    Ok(AdjacencyGraph {
        directed: !matches!(selection.direction, Direction::Undirected),
        node_ids,
        node_uuid_by_id,
        node_id_by_uuid,
        neighbors,
        node_vectors: HashMap::new(),
    })
}

/// Load one graph-native numeric-list property for every selected node.
pub(crate) fn load_node_vectors(
    graph: &mut AdjacencyGraph,
    dir: &Path,
    property_stems: &[String],
    property: &str,
) -> Result<(), GfError> {
    if graph.is_empty() {
        return Ok(());
    }
    validate_vector_shape(graph.node_ids.len(), 1)?;
    let mut values = HashMap::new();
    for stem in property_stems {
        for (uuid, row) in
            graphforge_storage::read_node_property_rows(dir, stem).map_err(storage_error)?
        {
            if !graph.node_id_by_uuid.contains_key(&uuid) {
                continue;
            }
            if let Some(value) = row.get(property)
                && let Some(previous) = values.insert(uuid, value.clone())
                && previous != *value
            {
                return Err(GfError::Validation(format!(
                    "node {} has conflicting vector property {property:?}",
                    uuid_text(&uuid)
                )));
            }
        }
    }

    let mut dimension = None;
    let mut vectors = HashMap::with_capacity(graph.node_ids.len());
    for &node_id in &graph.node_ids {
        let uuid = graph.node_uuid_by_id[&node_id];
        let value = values.get(&uuid).ok_or_else(|| {
            GfError::Validation(format!(
                "node {} is missing vector property {property:?}",
                uuid_text(&uuid)
            ))
        })?;
        let vector = decode_vector(property, &uuid, value)?;
        if let Some(expected) = dimension {
            if vector.len() != expected {
                return Err(GfError::Validation(format!(
                    "node {} vector property {property:?} has dimension {}; expected {expected}",
                    uuid_text(&uuid),
                    vector.len()
                )));
            }
        } else {
            dimension = Some(vector.len());
            validate_vector_shape(graph.node_ids.len(), vector.len())?;
        }
        vectors.insert(node_id, vector);
    }
    graph.replace_node_vectors(vectors)
}

/// Load one strict graph-native numeric property for every selected node.
pub(crate) fn load_node_numeric_property(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<HashMap<u64, f64>, GfError> {
    let mut values = HashMap::new();
    for stem in graphforge_storage::list_property_stems(dir) {
        for (uuid, row) in
            graphforge_storage::read_node_property_rows(dir, &stem).map_err(storage_error)?
        {
            let Some(&node_id) = graph.node_id_by_uuid.get(&uuid) else {
                continue;
            };
            let Some(value) = row.get(property) else {
                continue;
            };
            let number = match value {
                IrLiteral::Int(value) => exact_i64_as_f64(*value),
                IrLiteral::Float(value) if value.is_finite() => Some(*value),
                _ => None,
            }
            .ok_or_else(|| {
                GfError::Validation(format!(
                    "node {} property {property:?} must be finite and numeric",
                    uuid_text(&uuid)
                ))
            })?;
            if let Some(previous) = values.insert(node_id, number)
                && previous.total_cmp(&number) != std::cmp::Ordering::Equal
            {
                return Err(GfError::Validation(format!(
                    "node {} has conflicting property {property:?}",
                    uuid_text(&uuid)
                )));
            }
        }
    }
    for &node_id in graph.node_ids() {
        if !values.contains_key(&node_id) {
            return Err(GfError::Validation(format!(
                "node {} is missing property {property:?}",
                uuid_text(&graph.node_uuid_by_id[&node_id])
            )));
        }
    }
    Ok(values)
}

/// Load ordered scalar numeric properties as one feature vector per selected node.
pub(crate) fn load_node_scalar_features(
    graph: &mut AdjacencyGraph,
    dir: &Path,
    properties: &[String],
) -> Result<(), GfError> {
    if properties.is_empty() {
        graph.node_vectors.clear();
        return Ok(());
    }
    let columns = properties
        .iter()
        .map(|property| load_node_numeric_property(graph, dir, property))
        .collect::<Result<Vec<_>, _>>()?;
    let vectors = graph
        .node_ids
        .iter()
        .map(|&node_id| {
            let vector = columns
                .iter()
                .map(|column| column[&node_id])
                .collect::<Vec<_>>();
            (node_id, vector)
        })
        .collect();
    graph.node_vectors = vectors;
    Ok(())
}

/// Load ordered scalar-or-list numeric properties into one feature vector per node.
pub(crate) fn load_node_feature_properties(
    graph: &mut AdjacencyGraph,
    dir: &Path,
    properties: &[String],
) -> Result<(), GfError> {
    if graph.is_empty() {
        graph.node_vectors.clear();
        return Ok(());
    }
    let mut combined = graph
        .node_ids
        .iter()
        .map(|&node_id| (node_id, Vec::new()))
        .collect::<HashMap<_, _>>();
    for property in properties {
        let mut values = HashMap::new();
        for stem in graphforge_storage::list_property_stems(dir) {
            for (uuid, row) in
                graphforge_storage::read_node_property_rows(dir, &stem).map_err(storage_error)?
            {
                if !graph.node_id_by_uuid.contains_key(&uuid) {
                    continue;
                }
                if let Some(value) = row.get(property)
                    && let Some(previous) = values.insert(uuid, value.clone())
                    && previous != *value
                {
                    return Err(GfError::Validation(format!(
                        "node {} has conflicting feature property {property:?}",
                        uuid_text(&uuid)
                    )));
                }
            }
        }
        let mut width = None;
        for &node_id in &graph.node_ids {
            let uuid = graph.node_uuid_by_id[&node_id];
            let value = values.get(&uuid).ok_or_else(|| {
                GfError::Validation(format!(
                    "node {} is missing feature property {property:?}",
                    uuid_text(&uuid)
                ))
            })?;
            let feature = decode_feature(property, &uuid, value)?;
            if let Some(expected) = width {
                if feature.len() != expected {
                    return Err(GfError::Validation(format!(
                        "node {} feature property {property:?} has width {}; expected {expected}",
                        uuid_text(&uuid),
                        feature.len()
                    )));
                }
            } else {
                width = Some(feature.len());
            }
            combined
                .get_mut(&node_id)
                .expect("selected node has an initialized feature row")
                .extend(feature);
        }
    }
    graph.replace_node_vectors(combined)
}

fn decode_feature(property: &str, uuid: &[u8; 16], value: &IrLiteral) -> Result<Vec<f64>, GfError> {
    match value {
        IrLiteral::Int(value) => exact_i64_as_f64(*value)
            .map(|value| vec![value])
            .ok_or_else(|| invalid_feature(property, uuid)),
        IrLiteral::Float(value) if value.is_finite() => Ok(vec![*value]),
        IrLiteral::List(_) => decode_vector(property, uuid, value),
        _ => Err(invalid_feature(property, uuid)),
    }
}

fn invalid_feature(property: &str, uuid: &[u8; 16]) -> GfError {
    GfError::Validation(format!(
        "node {} feature property {property:?} must be a finite numeric scalar or non-empty fixed-length numeric list",
        uuid_text(uuid)
    ))
}

/// Resolve one graph-native partition property for every selected node.
///
/// Property tables are read above partition-aware kernels. The returned map
/// contains UUID identity and normalized scalar partition IDs only.
pub(crate) fn load_node_partition_property(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<ResolvedPartitionMap, GfError> {
    let mut rows = Vec::new();
    for stem in graphforge_storage::list_property_stems(dir) {
        rows.extend(
            graphforge_storage::read_node_property_rows(dir, &stem).map_err(storage_error)?,
        );
    }
    resolve_partition_rows(graph, property, rows)
}

fn resolve_partition_rows(
    graph: &AdjacencyGraph,
    property: &str,
    rows: impl IntoIterator<Item = ([u8; 16], HashMap<String, IrLiteral>)>,
) -> Result<ResolvedPartitionMap, GfError> {
    let mut values = HashMap::new();
    for (uuid, row) in rows {
        if graph.node_id(&uuid).is_some()
            && let Some(value) = row.get(property)
        {
            let value = partition_value(value);
            if let Some(previous) = values.insert(uuid, value.clone())
                && previous != value
            {
                return Err(GfError::Validation(format!(
                    "node {} has conflicting partition property {property:?}",
                    uuid_text(&uuid)
                )));
            }
        }
    }
    ResolvedPartitionMap::try_new(graph.node_uuids(), values)
        .map_err(|error| GfError::Validation(error.to_string()))
}

fn partition_value(value: &IrLiteral) -> PartitionValue {
    match value {
        IrLiteral::Str(value) => PartitionValue::String(value.clone()),
        IrLiteral::Int(value) => PartitionValue::Integer(*value),
        IrLiteral::Null => PartitionValue::Null,
        _ => PartitionValue::Unsupported("non-string/non-integer"),
    }
}

fn decode_vector(property: &str, uuid: &[u8; 16], value: &IrLiteral) -> Result<Vec<f64>, GfError> {
    let IrLiteral::List(items) = value else {
        return Err(invalid_vector(property, uuid, "must be a numeric list"));
    };
    if items.is_empty() {
        return Err(invalid_vector(property, uuid, "must not be empty"));
    }
    items
        .iter()
        .map(|item| {
            let number = match item {
                IrLiteral::Int(value) => exact_i64_as_f64(*value),
                IrLiteral::Float(value) if value.is_finite() => Some(*value),
                _ => None,
            }
            .ok_or_else(|| {
                invalid_vector(property, uuid, "must contain only finite numeric values")
            })?;
            Ok(number)
        })
        .collect()
}

fn invalid_vector(property: &str, uuid: &[u8; 16], reason: &str) -> GfError {
    GfError::Validation(format!(
        "node {} vector property {property:?} {reason}",
        uuid_text(uuid)
    ))
}

fn validate_vector_shape(nodes: usize, dimension: usize) -> Result<(), GfError> {
    if nodes > VECTOR_NODE_LIMIT {
        return Err(GfError::Validation(format!(
            "vector clustering selects {nodes} nodes; limit is {VECTOR_NODE_LIMIT}"
        )));
    }
    let cells = nodes
        .checked_mul(dimension)
        .ok_or_else(|| GfError::Validation("vector feature-cell count overflows usize".into()))?;
    if cells > VECTOR_CELL_LIMIT {
        return Err(GfError::Validation(format!(
            "vector clustering requires {cells} feature cells; limit is {VECTOR_CELL_LIMIT}"
        )));
    }
    Ok(())
}

fn selected_nodes(dir: &Path, label: Option<TypeId>) -> Result<(Vec<u64>, NodeUuidMap), GfError> {
    let mut rows = Vec::new();
    for batch in graphforge_storage::read_nodes(dir).map_err(storage_error)? {
        let uuids = fixed_binary(&batch, "node_uuid")?;
        let ids = uint64(&batch, "node_id")?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Execution("node topology type_ids is not List".into()))?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) || uuids.is_null(row) {
                continue;
            }
            if let Some(label) = label {
                let values = labels.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| {
                        GfError::Execution("node topology type_ids values are not UInt32".into())
                    })?;
                if !values.values().contains(&label.0) {
                    continue;
                }
            }
            rows.push((ids.value(row), uuid_at(uuids, row)?));
        }
    }
    rows.sort_unstable_by_key(|&(id, _)| id);
    let node_ids = rows.iter().map(|&(id, _)| id).collect();
    Ok((node_ids, rows.into_iter().collect()))
}

fn selected_edge_uuids(
    dir: &Path,
    mode: OntologyMode,
    via: &str,
    edge_ids: &HashSet<u64>,
) -> Result<HashMap<u64, [u8; 16]>, GfError> {
    let mut out = HashMap::new();
    // An advisory ontology can be adopted after exploratory writes. Resolve a
    // named relation from the union so UUID lookup sees both the original
    // `_exploratory.parquet` rows and any later typed rows, matching adjacency.
    let read_name = if matches!(mode, OntologyMode::Advisory) && via != "*" {
        "*"
    } else {
        via
    };
    for batch in graphforge_storage::read_edges_filtered(dir, read_name, mode, edge_ids)
        .map_err(storage_error)?
    {
        let ids = uint64(&batch, "edge_id")?;
        let uuids = fixed_binary(&batch, "edge_uuid")?;
        let relation_names = batch
            .column_by_name("rel_type_name")
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::StringArray>());
        for row in 0..batch.num_rows() {
            if via != "*" && relation_names.is_some_and(|names| names.value(row) != via) {
                continue;
            }
            if !ids.is_null(row) && !uuids.is_null(row) {
                out.insert(ids.value(row), uuid_at(uuids, row)?);
            }
        }
    }
    Ok(out)
}

fn selected_weights(
    dir: &Path,
    via: &str,
    property: &str,
    edge_uuids: &HashMap<u64, [u8; 16]>,
) -> Result<HashMap<[u8; 16], f64>, GfError> {
    let wanted: HashSet<[u8; 16]> = edge_uuids.values().copied().collect();
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let stems = if via == "*" {
        graphforge_storage::list_edge_property_stems(dir)
    } else {
        vec![via.to_owned()]
    };
    let mut out = HashMap::new();
    for stem in stems {
        for batch in graphforge_storage::read_edge_properties(dir, &stem).map_err(storage_error)? {
            let uuids = fixed_binary(&batch, "edge_uuid")?;
            let values = batch.column_by_name(property).ok_or_else(|| {
                GfError::Validation(format!("edge weight property {property:?} does not exist"))
            })?;
            for row in 0..batch.num_rows() {
                if uuids.is_null(row) {
                    continue;
                }
                let uuid = uuid_at(uuids, row)?;
                if wanted.contains(&uuid) && !values.is_null(row) {
                    out.insert(
                        uuid,
                        numeric_value(values.as_ref(), row).ok_or_else(|| {
                            GfError::Validation(format!(
                                "edge weight property {property:?} must be numeric"
                            ))
                        })?,
                    );
                }
            }
        }
    }
    Ok(out)
}

fn numeric_value(array: &dyn Array, row: usize) -> Option<f64> {
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        Some(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        Some(f64::from(values.value(row)))
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        exact_i64_as_f64(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        Some(f64::from(values.value(row)))
    } else if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        Some(f64::from(values.value(row)))
    } else if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        Some(f64::from(values.value(row)))
    } else if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        exact_u64_as_f64(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        Some(f64::from(values.value(row)))
    } else if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        Some(f64::from(values.value(row)))
    } else {
        array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|values| f64::from(values.value(row)))
    }
}

fn exact_i64_as_f64(value: i64) -> Option<f64> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
    (value.unsigned_abs() <= MAX_EXACT_INTEGER).then(|| {
        // Guarded by the exact IEEE-754 integer range above.
        #[allow(clippy::cast_precision_loss)]
        let converted = value as f64;
        converted
    })
}

fn exact_u64_as_f64(value: u64) -> Option<f64> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
    (value <= MAX_EXACT_INTEGER).then(|| {
        // Guarded by the exact IEEE-754 integer range above.
        #[allow(clippy::cast_precision_loss)]
        let converted = value as f64;
        converted
    })
}

fn fixed_binary<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| GfError::Execution(format!("{name} is not FixedSizeBinary")))
}

fn uint64<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a UInt64Array, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| GfError::Execution(format!("{name} is not UInt64")))
}

fn uuid_at(values: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    values
        .value(row)
        .try_into()
        .map_err(|_| GfError::Execution("UUID value is not 16 bytes".into()))
}

fn uuid_text(value: &[u8; 16]) -> String {
    graphforge_core::uuid::to_string(&graphforge_core::uuid::from_bytes(value))
}

fn storage_error(error: impl std::fmt::Display) -> GfError {
    GfError::Execution(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use graphforge_core::{
        algorithms::AnalyzeAlgorithm,
        embedding_options::{EmbeddingAnalyzeOptions, EmbeddingOptions, Node2VecOptions},
        uuid::{Uuid, new_v7, to_bytes},
    };
    use graphforge_ir::IrLiteral;
    use graphforge_storage::GraphWriter;
    use tempfile::TempDir;

    use super::*;
    use crate::adjacency::{PersistentAdjacencyProvider, ScanBuildAdjacencyProvider};
    use crate::algorithm_analyze::embedding_algorithm_with_controls;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmControl, AlgorithmLimits};
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;
    use crate::algorithm_embedding_options::normalize_embedding_options;

    const TS: i64 = 1_700_000_000_000_000;

    #[test]
    fn graph_feature_matrix_and_numeric_conversion_boundaries_are_exact() {
        let mut graph = AdjacencyGraph::from_resolved_projection(ResolvedGraphProjection {
            directed: true,
            nodes: vec![[1; 16], [2; 16]],
            edges: vec![ResolvedGraphEdge {
                edge_uuid: [3; 16],
                source_uuid: [1; 16],
                target_uuid: [2; 16],
                weight: 1.0,
            }],
        })
        .unwrap();
        assert_eq!(graph.node_ids(), &[0, 1]);
        assert_eq!(graph.node_id(&[1; 16]), Some(0));
        assert_eq!(graph.node_uuid(1), Some([2; 16]));
        assert!(graph.node_vector(0).is_none());
        assert!(!graph.is_empty());
        assert_eq!(graph.edge_entry_count(), 1);

        let error = graph
            .replace_node_vectors(HashMap::from([(0, vec![1.0])]))
            .unwrap_err();
        assert!(error.to_string().contains("feature matrix"));
        graph
            .replace_node_vectors(HashMap::from([(0, vec![1.0]), (1, vec![2.0])]))
            .unwrap();
        assert_eq!(graph.node_vector(1), Some([2.0].as_slice()));

        let exact = 1_i64 << 53;
        assert_eq!(exact_i64_as_f64(exact), Some(exact as f64));
        assert_eq!(exact_i64_as_f64(-exact), Some(-(exact as f64)));
        assert_eq!(exact_i64_as_f64(exact + 1), None);
        assert_eq!(exact_u64_as_f64(1_u64 << 53), Some((1_u64 << 53) as f64));
        assert_eq!(exact_u64_as_f64((1_u64 << 53) + 1), None);
        assert!(storage_error("sentinel").to_string().contains("sentinel"));
    }

    struct Fixture {
        dir: TempDir,
        uuids: [Uuid; 4],
        ids: [u64; 4],
        edges: [Uuid; 4],
    }

    fn fixture() -> Fixture {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let uuids = [new_v7(), new_v7(), new_v7(), new_v7()];
        let ids = [
            writer
                .create_node_with_labels(uuids[0], &[TypeId(1)])
                .unwrap(),
            writer
                .create_node_with_labels(uuids[1], &[TypeId(1), TypeId(2)])
                .unwrap(),
            writer
                .create_node_with_labels(uuids[2], &[TypeId(2)])
                .unwrap(),
            writer
                .create_node_with_labels(uuids[3], &[TypeId(1)])
                .unwrap(),
        ];
        let edges = [new_v7(), new_v7(), new_v7(), new_v7()];
        for (edge, source, target) in [
            (edges[0], uuids[0], uuids[1]),
            (edges[1], uuids[0], uuids[1]),
            (edges[2], uuids[1], uuids[2]),
            (edges[3], uuids[3], uuids[3]),
        ] {
            writer.create_edge(edge, "KNOWS", &source, &target).unwrap();
        }
        for (edge, weight) in [
            (edges[0], 2.5),
            (edges[1], -1.0),
            (edges[2], 3.0),
            (edges[3], 4.0),
        ] {
            writer
                .set_edge_properties(
                    &edge,
                    Some("KNOWS"),
                    HashMap::from([("cost".to_owned(), IrLiteral::Float(weight))]),
                )
                .unwrap();
        }
        for (index, uuid) in uuids.iter().enumerate() {
            let index = f64::from(u32::try_from(index).unwrap());
            writer
                .set_properties(
                    uuid,
                    Some("Person"),
                    HashMap::from([
                        (
                            "features".to_owned(),
                            IrLiteral::List(vec![
                                IrLiteral::Float(index),
                                IrLiteral::Float(index + 0.5),
                            ]),
                        ),
                        (
                            "side".to_owned(),
                            IrLiteral::Str(if index < 2.0 { "left" } else { "right" }.into()),
                        ),
                    ]),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        Fixture {
            dir,
            uuids,
            ids,
            edges,
        }
    }

    fn selection(direction: Direction) -> AdjacencySelection<'static> {
        AdjacencySelection {
            label: None,
            via: "KNOWS",
            direction,
            weight: None,
        }
    }

    fn resolved_edge(edge_uuid: Uuid, source_uuid: Uuid, target_uuid: Uuid) -> ResolvedGraphEdge {
        ResolvedGraphEdge {
            edge_uuid: to_bytes(&edge_uuid),
            source_uuid: to_bytes(&source_uuid),
            target_uuid: to_bytes(&target_uuid),
            weight: 1.0,
        }
    }

    #[test]
    fn resolved_projection_canonicalizes_multigraphs_and_rejects_invalid_identity() {
        let nodes = [[3; 16], [1; 16], [2; 16]];
        let edges = vec![
            ResolvedGraphEdge {
                edge_uuid: [2; 16],
                source_uuid: [1; 16],
                target_uuid: [2; 16],
                weight: -1.0,
            },
            ResolvedGraphEdge {
                edge_uuid: [1; 16],
                source_uuid: [1; 16],
                target_uuid: [2; 16],
                weight: 2.0,
            },
            ResolvedGraphEdge {
                edge_uuid: [3; 16],
                source_uuid: [3; 16],
                target_uuid: [3; 16],
                weight: 1.0,
            },
        ];
        let graph = AdjacencyGraph::from_resolved_projection(ResolvedGraphProjection {
            directed: false,
            nodes: nodes.to_vec(),
            edges: edges.clone(),
        })
        .unwrap();
        assert_eq!(
            graph.node_uuids().collect::<Vec<_>>(),
            [[1; 16], [2; 16], [3; 16]]
        );
        assert_eq!(graph.neighbors(0).len(), 2, "parallel edges");
        assert_eq!(graph.neighbors(2).len(), 2, "undirected loop mirrors");

        for (projection, expected) in [
            (
                ResolvedGraphProjection {
                    directed: true,
                    nodes: vec![[1; 16], [1; 16]],
                    edges: Vec::new(),
                },
                "duplicate node UUID",
            ),
            (
                ResolvedGraphProjection {
                    directed: true,
                    nodes: nodes.to_vec(),
                    edges: vec![
                        edges[0],
                        ResolvedGraphEdge {
                            source_uuid: [2; 16],
                            ..edges[0]
                        },
                    ],
                },
                "duplicate edge UUID",
            ),
            (
                ResolvedGraphProjection {
                    directed: true,
                    nodes: nodes.to_vec(),
                    edges: vec![ResolvedGraphEdge {
                        source_uuid: [9; 16],
                        ..edges[0]
                    }],
                },
                "source UUID is not selected",
            ),
            (
                ResolvedGraphProjection {
                    directed: true,
                    nodes: nodes.to_vec(),
                    edges: vec![ResolvedGraphEdge {
                        weight: f64::NAN,
                        ..edges[0]
                    }],
                },
                "weight must be finite",
            ),
        ] {
            let error = AdjacencyGraph::from_resolved_projection(projection).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn node2vec_native_and_equivalent_resolved_projection_are_exactly_equal() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let native = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        let resolved = AdjacencyGraph::from_resolved_projection(ResolvedGraphProjection {
            directed: true,
            nodes: [
                fixture.uuids[3],
                fixture.uuids[1],
                fixture.uuids[0],
                fixture.uuids[2],
            ]
            .into_iter()
            .map(|uuid| to_bytes(&uuid))
            .collect(),
            edges: vec![
                resolved_edge(fixture.edges[3], fixture.uuids[3], fixture.uuids[3]),
                resolved_edge(fixture.edges[2], fixture.uuids[1], fixture.uuids[2]),
                resolved_edge(fixture.edges[1], fixture.uuids[0], fixture.uuids[1]),
                resolved_edge(fixture.edges[0], fixture.uuids[0], fixture.uuids[1]),
            ],
        })
        .unwrap();
        let invocation = normalize_embedding_options(&EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::Node2Vec,
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 4,
                walk_length: 3,
                walks_per_node: 2,
                window_size: 1,
                negative_samples: 1,
                epochs: 1,
                seed: 7,
                ..Node2VecOptions::default()
            }),
        })
        .unwrap();
        let execute = |graph: &AdjacencyGraph| {
            embedding_algorithm_with_controls(
                graph,
                &invocation,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
                EmbeddingResourceLimits::default(),
            )
            .unwrap()
        };
        assert_eq!(
            native.projection_fingerprint().unwrap(),
            resolved.projection_fingerprint().unwrap()
        );
        assert_eq!(execute(&native), execute(&resolved));
    }

    #[test]
    fn projection_fingerprint_tracks_public_topology_direction_and_weight() {
        let projection = ResolvedGraphProjection {
            directed: true,
            nodes: vec![[2; 16], [1; 16]],
            edges: vec![ResolvedGraphEdge {
                edge_uuid: [3; 16],
                source_uuid: [1; 16],
                target_uuid: [2; 16],
                weight: 1.0,
            }],
        };
        let fingerprint = |projection| {
            AdjacencyGraph::from_resolved_projection(projection)
                .unwrap()
                .projection_fingerprint()
                .unwrap()
        };
        let baseline = fingerprint(projection.clone());
        assert_ne!(baseline.as_bytes(), &[0; 32]);
        assert_ne!(
            baseline,
            fingerprint(ResolvedGraphProjection {
                directed: false,
                ..projection.clone()
            })
        );
        assert_ne!(
            baseline,
            fingerprint(ResolvedGraphProjection {
                edges: vec![ResolvedGraphEdge {
                    weight: 2.0,
                    ..projection.edges[0]
                }],
                ..projection.clone()
            })
        );
        assert_ne!(
            baseline,
            fingerprint(ResolvedGraphProjection {
                nodes: vec![[4; 16], [1; 16]],
                edges: vec![ResolvedGraphEdge {
                    target_uuid: [4; 16],
                    ..projection.edges[0]
                }],
                ..projection
            })
        );

        let missing_mirror = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(
            missing_mirror
                .projection_fingerprint()
                .unwrap_err()
                .to_string()
                .contains("missing its mirrored adjacency")
        );
    }

    #[test]
    fn direction_parallel_self_loop_and_uuid_round_trip() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);

        let out = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        assert_eq!(out.node_ids(), fixture.ids);
        assert_eq!(
            out.node_uuids().collect::<Vec<_>>(),
            fixture.uuids.iter().map(to_bytes).collect::<Vec<_>>()
        );
        assert_eq!(out.neighbors(fixture.ids[0]).len(), 2, "parallel edges");
        assert_eq!(out.neighbors(fixture.ids[3]).len(), 1, "out self-loop");
        assert_eq!(
            out.node_uuid(fixture.ids[1]),
            Some(to_bytes(&fixture.uuids[1]))
        );
        assert_eq!(
            out.node_id(&to_bytes(&fixture.uuids[1])),
            Some(fixture.ids[1])
        );
        assert_eq!(
            out.neighbors(fixture.ids[0])[0].edge_uuid,
            to_bytes(&fixture.edges[0])
        );

        let incoming = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::In),
        )
        .unwrap();
        assert_eq!(incoming.neighbors(fixture.ids[1]).len(), 2);

        let undirected = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Undirected),
        )
        .unwrap();
        assert_eq!(undirected.neighbors(fixture.ids[3]).len(), 2);
    }

    #[test]
    fn label_filter_excludes_nonmatching_endpoints() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            AdjacencySelection {
                label: Some(TypeId(1)),
                ..selection(Direction::Out)
            },
        )
        .unwrap();
        assert_eq!(
            graph.node_ids(),
            &[fixture.ids[0], fixture.ids[1], fixture.ids[3]]
        );
        assert!(graph.neighbors(fixture.ids[1]).is_empty());
        assert_eq!(graph.neighbors(fixture.ids[0]).len(), 2);
    }

    #[test]
    fn selected_weights_preserve_negative_values() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            AdjacencySelection {
                weight: Some("cost"),
                ..selection(Direction::Out)
            },
        )
        .unwrap();
        assert_eq!(
            graph
                .neighbors(fixture.ids[0])
                .iter()
                .map(|edge| edge.weight)
                .collect::<Vec<_>>(),
            vec![2.5, -1.0]
        );
    }

    #[test]
    fn missing_weight_is_a_validation_error() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let error = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            AdjacencySelection {
                weight: Some("missing"),
                ..selection(Direction::Out)
            },
        )
        .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
    }

    #[test]
    fn empty_graph_keeps_a_valid_empty_mapping() {
        let dir = TempDir::new().unwrap();
        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        assert!(graph.is_empty());
        assert!(graph.neighbors(42).is_empty());
        load_node_vectors(&mut graph, dir.path(), &[], "features").unwrap();
    }

    #[test]
    fn graph_native_vectors_load_by_uuid_in_topology_order() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        load_node_vectors(
            &mut graph,
            fixture.dir.path(),
            &["Person".into()],
            "features",
        )
        .unwrap();

        for (index, &node_id) in fixture.ids.iter().enumerate() {
            let index = f64::from(u32::try_from(index).unwrap());
            assert_eq!(
                graph.node_vector(node_id),
                Some([index, index + 0.5].as_slice())
            );
        }
        assert!(!fixture.dir.path().join("knowledge").exists());
    }

    #[test]
    fn graphsage_feature_loader_preserves_property_order_and_is_atomic() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        load_node_feature_properties(&mut graph, fixture.dir.path(), &["features".into()]).unwrap();
        let before = fixture
            .ids
            .iter()
            .map(|&node_id| graph.node_vector(node_id).unwrap().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(before[0], [0.0, 0.5]);
        assert_eq!(before[1], [1.0, 1.5]);

        assert!(matches!(
            load_node_feature_properties(
                &mut graph,
                fixture.dir.path(),
                &["features".into(), "missing".into()]
            ),
            Err(GfError::Validation(_))
        ));
        assert_eq!(
            fixture
                .ids
                .iter()
                .map(|&node_id| graph.node_vector(node_id).unwrap().to_vec())
                .collect::<Vec<_>>(),
            before
        );
    }

    #[test]
    fn graph_native_partition_property_loads_without_knowledge_storage() {
        let fixture = fixture();
        let graph = export_node_selection(fixture.dir.path(), None).unwrap();
        let mapping = load_node_partition_property(&graph, fixture.dir.path(), "side").unwrap();
        assert_eq!(mapping.iter().count(), fixture.uuids.len());
        assert_eq!(
            mapping
                .get(&to_bytes(&fixture.uuids[0]))
                .map(|partition| partition.as_str()),
            Some("left")
        );
        assert!(!fixture.dir.path().join("knowledge").exists());
    }

    #[test]
    fn vector_values_and_dense_guards_are_typed() {
        let uuid = [0; 16];
        assert_eq!(
            decode_feature("score", &uuid, &IrLiteral::Int(7)).unwrap(),
            [7.0]
        );
        assert_eq!(
            decode_feature(
                "features",
                &uuid,
                &IrLiteral::List(vec![IrLiteral::Float(1.0), IrLiteral::Int(2)])
            )
            .unwrap(),
            [1.0, 2.0]
        );
        assert_eq!(
            decode_vector(
                "features",
                &uuid,
                &IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Float(2.5)])
            )
            .unwrap(),
            [1.0, 2.5]
        );
        for value in [
            IrLiteral::Null,
            IrLiteral::Int(1),
            IrLiteral::List(vec![]),
            IrLiteral::List(vec![IrLiteral::Null]),
            IrLiteral::List(vec![IrLiteral::Str("x".into())]),
            IrLiteral::List(vec![IrLiteral::List(vec![IrLiteral::Int(1)])]),
            IrLiteral::List(vec![IrLiteral::Float(f64::NAN)]),
            IrLiteral::List(vec![IrLiteral::Float(f64::INFINITY)]),
            IrLiteral::List(vec![IrLiteral::Int((1_i64 << 53) + 1)]),
        ] {
            assert!(matches!(
                decode_vector("features", &uuid, &value),
                Err(GfError::Validation(_))
            ));
        }
        for value in [
            IrLiteral::Null,
            IrLiteral::Bool(true),
            IrLiteral::Str("x".into()),
            IrLiteral::Float(f64::NAN),
            IrLiteral::List(vec![]),
            IrLiteral::List(vec![IrLiteral::Null]),
        ] {
            assert!(matches!(
                decode_feature("features", &uuid, &value),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            validate_vector_shape(VECTOR_NODE_LIMIT + 1, 1),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            validate_vector_shape(VECTOR_NODE_LIMIT, VECTOR_NODE_LIMIT + 1),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            validate_vector_shape(VECTOR_NODE_LIMIT, usize::MAX),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn partition_rows_resolve_selected_string_and_integer_values() {
        let graph = AdjacencyGraph::with_test_counts(2, 0);
        let mapping = resolve_partition_rows(
            &graph,
            "side",
            [
                (
                    u128::from(1_u8).to_be_bytes(),
                    HashMap::from([("side".into(), IrLiteral::Int(7))]),
                ),
                (
                    u128::from(0_u8).to_be_bytes(),
                    HashMap::from([("side".into(), IrLiteral::Str("left".into()))]),
                ),
                (
                    u128::from(9_u8).to_be_bytes(),
                    HashMap::from([("side".into(), IrLiteral::Null)]),
                ),
            ],
        )
        .unwrap();
        assert_eq!(
            mapping
                .get(&u128::from(0_u8).to_be_bytes())
                .map(|partition| partition.as_str()),
            Some("left")
        );
        assert_eq!(
            mapping
                .get(&u128::from(1_u8).to_be_bytes())
                .map(|partition| partition.as_str()),
            Some("7")
        );
    }

    #[test]
    fn partition_rows_reject_missing_null_unsupported_and_conflicting_values() {
        let graph = AdjacencyGraph::with_test_counts(1, 0);
        for value in [IrLiteral::Null, IrLiteral::Bool(true)] {
            assert!(matches!(
                resolve_partition_rows(
                    &graph,
                    "side",
                    [(
                        u128::from(0_u8).to_be_bytes(),
                        HashMap::from([("side".into(), value)])
                    )]
                ),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            resolve_partition_rows(&graph, "side", []),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            resolve_partition_rows(
                &graph,
                "side",
                [
                    (
                        u128::from(0_u8).to_be_bytes(),
                        HashMap::from([("side".into(), IrLiteral::Int(0))])
                    ),
                    (
                        u128::from(0_u8).to_be_bytes(),
                        HashMap::from([("side".into(), IrLiteral::Int(1))])
                    )
                ]
            ),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn missing_and_ragged_vectors_fail_without_partial_attachment() {
        let fixture = fixture();
        let provider =
            ScanBuildAdjacencyProvider::new(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        assert!(matches!(
            load_node_vectors(
                &mut graph,
                fixture.dir.path(),
                &["Person".into()],
                "missing"
            ),
            Err(GfError::Validation(_))
        ));
        assert!(
            fixture
                .ids
                .iter()
                .all(|&node_id| graph.node_vector(node_id).is_none())
        );

        graphforge_storage::set_node_properties(
            fixture.dir.path(),
            "Person",
            &HashMap::from([(
                to_bytes(&fixture.uuids[1]),
                HashMap::from([(
                    "features".into(),
                    IrLiteral::List(vec![IrLiteral::Float(1.0)]),
                )]),
            )]),
        )
        .unwrap();
        assert!(matches!(
            load_node_vectors(
                &mut graph,
                fixture.dir.path(),
                &["Person".into()],
                "features"
            ),
            Err(GfError::Validation(_))
        ));
        assert!(
            fixture
                .ids
                .iter()
                .all(|&node_id| graph.node_vector(node_id).is_none())
        );
    }

    #[test]
    fn stale_persistent_index_falls_back_without_semantic_drift() {
        let fixture = fixture();
        graphforge_storage::adjacency::build_adjacency_index(fixture.dir.path(), TS).unwrap();

        let extra = new_v7();
        let mut writer =
            GraphWriter::open_at(fixture.dir.path(), OntologyMode::Strict, TS + 1).unwrap();
        for (&uuid, &id) in fixture.uuids.iter().zip(&fixture.ids) {
            writer.register_existing_node(uuid, id);
        }
        writer
            .create_edge(extra, "KNOWS", &fixture.uuids[1], &fixture.uuids[0])
            .unwrap();
        writer.flush().unwrap();

        let provider = PersistentAdjacencyProvider::new(
            fixture.dir.path().to_path_buf(),
            OntologyMode::Strict,
        );
        let graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        assert_eq!(graph.neighbors(fixture.ids[1]).len(), 2);
    }

    #[test]
    fn projection_fingerprint_rejects_corrupt_topology_and_vectors() {
        let uuid0 = u128::from(10_u8).to_be_bytes();
        let uuid1 = u128::from(11_u8).to_be_bytes();
        let edge_uuid = u128::from(12_u8).to_be_bytes();
        let base = || AdjacencyGraph {
            directed: true,
            node_ids: vec![0, 1],
            node_uuid_by_id: HashMap::from([(0, uuid0), (1, uuid1)]),
            node_id_by_uuid: HashMap::from([(uuid0, 0), (uuid1, 1)]),
            neighbors: HashMap::from([(
                0,
                vec![AlgorithmEdge {
                    edge_id: 0,
                    neighbor_id: 1,
                    edge_uuid,
                    weight: 2.5,
                }],
            )]),
            node_vectors: HashMap::new(),
        };

        let valid = base().projection_fingerprint().expect("valid projection");
        assert_ne!(valid.as_bytes(), &[0; 32]);

        let mut missing_target = base();
        missing_target.neighbors.get_mut(&0).unwrap()[0].neighbor_id = 9;
        assert_eq!(
            missing_target
                .projection_fingerprint()
                .unwrap_err()
                .to_string(),
            "execution error: algorithm projection target has no UUID identity"
        );

        let mut non_finite = base();
        non_finite.neighbors.get_mut(&0).unwrap()[0].weight = f64::NAN;
        assert_eq!(
            non_finite.projection_fingerprint().unwrap_err().to_string(),
            "execution error: algorithm projection edge weight is not finite"
        );

        let mut unmirrored = base();
        unmirrored.directed = false;
        assert_eq!(
            unmirrored.projection_fingerprint().unwrap_err().to_string(),
            "execution error: undirected algorithm projection edge is missing its mirrored adjacency"
        );

        let mut vectors = base();
        vectors.node_vectors.insert(0, vec![1.0, 2.0]);
        vectors.node_vectors.insert(1, vec![3.0, 4.0]);
        let fingerprint = vectors
            .descriptor_projection_fingerprint()
            .expect("finite descriptor projection");
        assert_ne!(fingerprint.as_bytes(), valid.as_bytes());

        vectors.node_vectors.get_mut(&1).unwrap()[0] = f64::INFINITY;
        assert_eq!(
            vectors
                .descriptor_projection_fingerprint()
                .unwrap_err()
                .to_string(),
            "execution error: algorithm vector projection contains a non-finite value"
        );
        vectors.node_vectors.remove(&1);
        vectors.node_vectors.insert(9, vec![1.0]);
        assert_eq!(
            vectors
                .descriptor_projection_fingerprint()
                .unwrap_err()
                .to_string(),
            "execution error: algorithm vector projection has no UUID identity"
        );
    }
}
