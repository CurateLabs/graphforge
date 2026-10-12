//! Rust-owned adapter from [`AdjacencyProvider`] to the graph consumed by algorithm.
//!
//! The provider remains the sole adjacency implementation. This module adds
//! stable UUID metadata, optional label selection, and optional edge weights;
//! it never constructs adjacency from Parquet topology rows.
#![allow(
    dead_code,
    reason = "algorithm foundation consumed by the Rust dispatch slice in issue #1146"
)]

use graphforge_value::EntityTypeSelection;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use graphforge_core::hash_observation::ContractSha256 as Sha256;
use graphforge_core::{GfError, OntologyMode};
use graphforge_ir::{Direction, IrLiteral};
#[cfg(test)]
use graphforge_value::EntityTypeId;
use sha2::Digest;

use crate::adjacency::AdjacencyProvider;
use crate::algorithm_partition::{PartitionValue, ResolvedPartitionMap};

mod topology;

#[cfg(test)]
mod export_tests;
#[cfg(all(test, feature = "algorithms-extended"))]
mod parity_tests;

use topology::{
    EdgeIdentity, NodeSet, TopologyBuilder, TopologyGraph, resolve_edge_uuids, resolve_weights,
    select_nodes,
};

const VECTOR_NODE_LIMIT: usize = 4_096;
const VECTOR_CELL_LIMIT: usize = 16_777_216;

/// Selection applied while exporting the shared adjacency view.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdjacencySelection<'a> {
    /// Resolved node label id. `None` includes every node.
    pub label: EntityTypeSelection,
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

/// Knowledge-agnostic graph projection resolved before algorithm dispatch.
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

/// Versioned digest of the exact public-identity projection consumed by algorithm.
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
///
/// The structure is a [`TopologyGraph`]: a CSR over `u32` node ordinals plus
/// parallel UUID, weight and edge-identity arrays, with no map keyed by an edge
/// or a node (#1974). This type is the surrogate-keyed view the algorithm
/// kernels were written against. [`Self::neighbors`] materializes
/// [`AlgorithmEdge`]s on demand, so kernels that only walk neighbors should
/// prefer [`Self::neighbor_ordinals`] and [`Self::neighbor_weights`].
#[derive(Clone, Debug, Default)]
pub(crate) struct AdjacencyGraph {
    topology: TopologyGraph,
    /// `node_ids()` of a dense graph, materialized on first use.
    dense_node_ids: OnceLock<Vec<u64>>,
    uuid_lookup: UuidLookup,
    node_vectors: HashMap<u64, Vec<f64>>,
    node_initial_labels: HashMap<u64, i64>,
    inventory: Option<std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory>>,
    evidence: AlgorithmExportEvidence,
}

/// What an export read and expanded, as structural evidence that it did not
/// build the edge-scale state the previous export did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AlgorithmExportEvidence {
    /// Entries held in hash maps by the adjacency view the export read. A
    /// persisted CSR serves every row without any; a scan-built fallback view
    /// holds one per adjacency entry. The export builds no hash map of its own.
    pub(crate) edge_scale_hash_entries: u64,
    /// Edge-table rows the export streamed to resolve edge identity or weights.
    /// Zero when the export needs neither.
    pub(crate) edge_rows_read: u64,
}

/// Which per-edge facts an export keeps besides the topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EdgeIdentityDemand {
    /// Topology (and weights, when requested) only. No edge table is read
    /// unless a weight is requested.
    Omit,
    /// Also keep each slot's edge id and UUID, for kernels that report edges.
    Retain,
}

/// Node lookups by UUID without a hash map.
///
/// The paths verbs look up one or two UUIDs, which a scan answers without
/// building anything. A caller that keeps looking (the property loaders do one
/// per property row) gets a UUID-sorted permutation, built once.
#[derive(Debug, Default)]
struct UuidLookup {
    scans: AtomicUsize,
    sorted: OnceLock<Vec<u32>>,
}

/// Lookups answered by a scan before a sorted permutation is built.
const LINEAR_UUID_LOOKUPS: usize = 2;

impl Clone for UuidLookup {
    fn clone(&self) -> Self {
        Self {
            scans: AtomicUsize::new(self.scans.load(Ordering::Relaxed)),
            sorted: self.sorted.clone(),
        }
    }
}

impl UuidLookup {
    /// Ordinal of the first node with `target`, if any.
    fn find(&self, uuids: &[[u8; 16]], target: &[u8; 16]) -> Option<usize> {
        let sorted = match self.sorted.get() {
            Some(sorted) => sorted,
            None if self.scans.fetch_add(1, Ordering::Relaxed) < LINEAR_UUID_LOOKUPS => {
                return uuids.iter().position(|uuid| uuid == target);
            }
            None => self.sorted.get_or_init(|| {
                let mut order: Vec<u32> = (0..uuids.len())
                    .map(|ordinal| u32::try_from(ordinal).unwrap_or(u32::MAX))
                    .collect();
                order.sort_unstable_by_key(|&ordinal| (uuids[ordinal as usize], ordinal));
                order
            }),
        };
        let first = sorted.partition_point(|&ordinal| uuids[ordinal as usize] < *target);
        sorted
            .get(first)
            .map(|&ordinal| ordinal as usize)
            .filter(|&ordinal| uuids[ordinal] == *target)
    }
}

/// The adjacency entries of one node, materialized as [`AlgorithmEdge`]s on
/// access from the ordinal topology and its edge identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NeighborView<'a> {
    graph: &'a AdjacencyGraph,
    start: usize,
    end: usize,
}

impl<'a> NeighborView<'a> {
    /// Number of entries.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether the node has no entries.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// Entry `index`, if present.
    #[must_use]
    pub(crate) fn get(&self, index: usize) -> Option<AlgorithmEdge> {
        (index < self.len()).then(|| self.graph.algorithm_edge(self.start + index))
    }

    /// Entries in order.
    #[must_use]
    pub(crate) fn iter(&self) -> NeighborIter<'a> {
        NeighborIter {
            graph: self.graph,
            front: self.start,
            back: self.end,
        }
    }

    /// Materialize the entries.
    #[must_use]
    pub(crate) fn to_vec(self) -> Vec<AlgorithmEdge> {
        self.iter().collect()
    }
}

/// Iterator over a [`NeighborView`].
#[derive(Clone, Debug)]
pub(crate) struct NeighborIter<'a> {
    graph: &'a AdjacencyGraph,
    front: usize,
    back: usize,
}

impl Iterator for NeighborIter<'_> {
    type Item = AlgorithmEdge;

    fn next(&mut self) -> Option<AlgorithmEdge> {
        (self.front < self.back).then(|| {
            let edge = self.graph.algorithm_edge(self.front);
            self.front += 1;
            edge
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for NeighborIter<'_> {
    fn next_back(&mut self) -> Option<AlgorithmEdge> {
        (self.front < self.back).then(|| {
            self.back -= 1;
            self.graph.algorithm_edge(self.back)
        })
    }
}

impl ExactSizeIterator for NeighborIter<'_> {}

impl<'a> IntoIterator for NeighborView<'a> {
    type Item = AlgorithmEdge;
    type IntoIter = NeighborIter<'a>;

    fn into_iter(self) -> NeighborIter<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &NeighborView<'a> {
    type Item = AlgorithmEdge;
    type IntoIter = NeighborIter<'a>;

    fn into_iter(self) -> NeighborIter<'a> {
        self.iter()
    }
}

impl AdjacencyGraph {
    /// Wrap an exported topology with the authority it was read under.
    fn from_topology(
        topology: TopologyGraph,
        inventory: Option<std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory>>,
        evidence: AlgorithmExportEvidence,
    ) -> Self {
        Self {
            topology,
            dense_node_ids: OnceLock::new(),
            uuid_lookup: UuidLookup::default(),
            node_vectors: HashMap::new(),
            node_initial_labels: HashMap::new(),
            inventory,
            evidence,
        }
    }

    /// Build a graph from per-node entries `(node_id, entry)`, with the node ids
    /// ascending and `uuids[n]` the UUID of the `n`th. Entries are ordered by
    /// `(edge_id, neighbor_id)` within a node. This is the construction used by
    /// resolved projections and by fixtures; an export never goes through it.
    /// A neighbor that is not one of `node_ids` is kept as an unresolvable
    /// ordinal so malformed fixtures stay malformed.
    fn from_entries(
        directed: bool,
        node_ids: Vec<u64>,
        uuids: Vec<[u8; 16]>,
        mut entries: Vec<(u64, AlgorithmEdge)>,
    ) -> Self {
        entries.sort_by_key(|(node, edge)| (*node, edge.edge_id, edge.neighbor_id));
        let nodes = NodeSet::from_ids(node_ids, u64::MAX);
        let node_count = nodes.len();
        let unresolvable = u32::try_from(node_count).unwrap_or(u32::MAX);
        let mut counts = vec![0_u32; node_count];
        let mut neighbors = Vec::with_capacity(entries.len());
        let mut identity = EdgeIdentity::default();
        let mut weights = Vec::with_capacity(entries.len());
        let mut sources = Vec::with_capacity(entries.len());
        for (node, edge) in &entries {
            let Some(source) = nodes.ordinal_of(*node) else {
                continue;
            };
            counts[source] += 1;
            sources.push(source);
            neighbors.push(
                nodes
                    .ordinal_of(edge.neighbor_id)
                    .and_then(|ordinal| u32::try_from(ordinal).ok())
                    .unwrap_or(unresolvable),
            );
            identity.edge_ids.push(edge.edge_id);
            identity.edge_uuids.push(edge.edge_uuid);
            weights.push(edge.weight);
        }
        debug_assert!(sources.windows(2).all(|pair| pair[0] <= pair[1]));
        let mut offsets = Vec::with_capacity(node_count + 1);
        offsets.push(0);
        for count in counts {
            offsets.push(offsets.last().copied().unwrap_or(0) + count);
        }
        Self::from_topology(
            TopologyGraph {
                directed,
                offsets,
                neighbors,
                weights: Some(weights),
                uuids,
                nodes,
                identity: Some(identity),
            },
            None,
            AlgorithmExportEvidence::default(),
        )
    }

    fn algorithm_edge(&self, slot: usize) -> AlgorithmEdge {
        let topology = &self.topology;
        let identity = topology.identity.as_ref().expect(
            "AdjacencyGraph::neighbors needs an export that retained edge identity; \
             an identity-free export is read through neighbor_ordinals",
        );
        AlgorithmEdge {
            edge_id: identity.edge_ids[slot],
            edge_uuid: identity.edge_uuids[slot],
            neighbor_id: topology.node_id_of(topology.neighbors[slot] as usize),
            weight: topology
                .weights
                .as_ref()
                .map_or(1.0, |weights| weights[slot]),
        }
    }

    /// The ordinal topology behind this view.
    pub(crate) fn topology(&self) -> &TopologyGraph {
        &self.topology
    }

    /// What the export that produced this graph read and expanded.
    pub(crate) fn export_evidence(&self) -> AlgorithmExportEvidence {
        self.evidence
    }

    /// Number of selected nodes.
    pub(crate) fn node_count(&self) -> usize {
        self.topology.node_count()
    }

    /// Ordinal of `node_id` among the selected nodes, in ascending id order.
    pub(crate) fn ordinal_of(&self, node_id: u64) -> Option<usize> {
        self.topology.ordinal_of(node_id)
    }

    /// Surrogate `node_id` of the selected node at `ordinal`.
    pub(crate) fn node_id_of_ordinal(&self, ordinal: usize) -> u64 {
        self.topology.node_id_of(ordinal)
    }

    /// Neighbor ordinals of the node at `ordinal`, in adjacency order.
    pub(crate) fn neighbor_ordinals(&self, ordinal: usize) -> &[u32] {
        self.topology.neighbor_ordinals(ordinal)
    }

    /// Edge weights aligned with [`Self::neighbor_ordinals`], when a weight
    /// property was requested.
    pub(crate) fn neighbor_weights(&self, ordinal: usize) -> Option<&[f64]> {
        self.topology.neighbor_weights(ordinal)
    }

    pub(crate) fn retain_inventory(
        &mut self,
        inventory: Option<std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory>>,
    ) {
        self.inventory = inventory;
    }

    pub(crate) fn property_routes(&self, dir: &Path, edge: bool) -> Vec<String> {
        match &self.inventory {
            Some(inventory) => inventory
                .routes(if edge {
                    graphforge_storage::PropertyRouteKind::Edge
                } else {
                    graphforge_storage::PropertyRouteKind::Node
                })
                .map(str::to_owned)
                .collect(),
            None if edge => graphforge_storage::list_edge_property_stems(dir),
            None => graphforge_storage::list_property_stems(dir),
        }
    }

    pub(crate) fn node_property_rows(
        &self,
        dir: &Path,
        route: &str,
    ) -> Result<HashMap<[u8; 16], HashMap<String, IrLiteral>>, GfError> {
        match &self.inventory {
            Some(inventory) => {
                graphforge_storage::read_node_property_rows_from_inventory(dir, inventory, route)
            }
            None => graphforge_storage::read_node_property_rows(dir, route),
        }
    }

    pub(crate) fn edge_property_batches(
        &self,
        dir: &Path,
        route: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, GfError> {
        match &self.inventory {
            Some(inventory) => {
                graphforge_storage::read_edge_properties_from_inventory(dir, inventory, route)
            }
            None => graphforge_storage::read_edge_properties(dir, route),
        }
        .map_err(storage_error)
    }

    #[cfg(test)]
    pub(crate) fn malformed_for_defensive_tests(
        directed: bool,
        node_ids: Vec<u64>,
        node_uuid_by_id: HashMap<u64, [u8; 16]>,
        neighbors: HashMap<u64, Vec<AlgorithmEdge>>,
    ) -> Self {
        // The UUIDs run in node order and stop at the first node without one,
        // which leaves the later nodes unresolvable.
        let uuids = node_ids
            .iter()
            .map_while(|node_id| node_uuid_by_id.get(node_id).copied())
            .collect();
        let entries = neighbors
            .into_iter()
            .flat_map(|(node, edges)| edges.into_iter().map(move |edge| (node, edge)))
            .collect();
        Self::from_entries(directed, node_ids, uuids, entries)
    }

    /// Fingerprint public UUID topology without execution surrogates.
    pub(crate) fn projection_fingerprint(&self) -> Result<AlgorithmProjectionFingerprint, GfError> {
        let mut nodes = self.node_uuids().collect::<Vec<_>>();
        nodes.sort_unstable();
        let node_count = u64::try_from(nodes.len()).map_err(|_| {
            GfError::Execution("algorithm projection node count exceeds UInt64 range".into())
        })?;

        if self.topology.identity.is_none() && !self.topology.neighbors.is_empty() {
            return Err(GfError::Execution(
                "algorithm projection fingerprint needs an export that retained edge identity"
                    .into(),
            ));
        }
        let directed = self.is_directed();
        let mut logical_edges: BTreeMap<[u8; 16], LogicalProjectionEdge> = BTreeMap::new();
        for &source_id in self.node_ids() {
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
                let (source, target) = if directed || source_uuid <= target_uuid {
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
                        if directed
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
        if !directed && logical_edges.values().any(|edge| edge.mirror_count != 2) {
            return Err(GfError::Execution(
                "undirected algorithm projection edge is missing its mirrored adjacency".into(),
            ));
        }
        let edge_count = u64::try_from(logical_edges.len()).map_err(|_| {
            GfError::Execution("algorithm projection edge count exceeds UInt64 range".into())
        })?;

        let mut digest = Sha256::new();
        digest.update(b"graphforge_algorithm_projection_v1");
        digest.update([u8::from(directed)]);
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
        if !self.node_initial_labels.is_empty() {
            let mut labels = self
                .node_initial_labels
                .iter()
                .map(|(&node, &label)| {
                    self.node_uuid(node)
                        .map(|uuid| (uuid, label))
                        .ok_or_else(|| {
                            GfError::Execution(
                                "initial label projection has no UUID identity".into(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            labels.sort_unstable_by_key(|(uuid, _)| *uuid);
            digest.update(b"initial_int64_labels");
            for (uuid, label) in labels {
                digest.update(uuid);
                digest.update(label.to_be_bytes());
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
        let ordinal_of = |uuid: &[u8; 16]| projection.nodes.binary_search(uuid).ok();

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
        let mut entries = Vec::with_capacity(projection.edges.len() * 2);
        for (index, edge) in projection.edges.iter().enumerate() {
            if !edge.weight.is_finite() {
                return Err(GfError::Validation(
                    "resolved graph projection edge weight must be finite".into(),
                ));
            }
            let source = ordinal_of(&edge.source_uuid).ok_or_else(|| {
                GfError::Validation(
                    "resolved graph projection edge source UUID is not selected".into(),
                )
            })?;
            let target = ordinal_of(&edge.target_uuid).ok_or_else(|| {
                GfError::Validation(
                    "resolved graph projection edge target UUID is not selected".into(),
                )
            })?;
            let edge_id = u64::try_from(index).map_err(|_| {
                GfError::Validation(
                    "resolved graph projection edge count exceeds UInt64 range".into(),
                )
            })?;
            let (source, target) = (node_ids[source], node_ids[target]);
            let entry = |neighbor_id| AlgorithmEdge {
                edge_id,
                edge_uuid: edge.edge_uuid,
                neighbor_id,
                weight: edge.weight,
            };
            entries.push((source, entry(target)));
            if !projection.directed {
                entries.push((target, entry(source)));
            }
        }
        Ok(Self::from_entries(
            projection.directed,
            node_ids,
            projection.nodes.clone(),
            entries,
        ))
    }

    /// Whether the exported topology preserves edge orientation.
    #[must_use]
    pub(crate) fn is_directed(&self) -> bool {
        self.topology.directed
    }

    /// Selected public node identities in deterministic surrogate order.
    pub(crate) fn node_uuids(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.topology.uuids.iter().copied()
    }

    /// Selected node surrogates in ascending order. A graph over consecutive ids
    /// has no stored id array; the slice is built on the first call, so a kernel
    /// that walks ordinals never pays for it.
    #[must_use]
    pub(crate) fn node_ids(&self) -> &[u64] {
        match &self.topology.nodes {
            NodeSet::Sparse { ids, .. } => ids,
            NodeSet::Dense { first, count } => self.dense_node_ids.get_or_init(|| {
                (0..*count as u64)
                    .map(|ordinal| first.saturating_add(ordinal))
                    .collect()
            }),
        }
    }

    /// Adjacency entries for `node_id`, ordered by `(edge_id, neighbor_id)`.
    ///
    /// Entries are built on access from the ordinal topology. This needs an
    /// export that retained edge identity.
    #[must_use]
    pub(crate) fn neighbors(&self, node_id: u64) -> NeighborView<'_> {
        let slots = self
            .topology
            .ordinal_of(node_id)
            .map_or(0..0, |ordinal| self.topology.slots(ordinal));
        NeighborView {
            graph: self,
            start: slots.start,
            end: slots.end,
        }
    }

    /// Resolve an internal node surrogate to its public UUID.
    #[must_use]
    pub(crate) fn node_uuid(&self, node_id: u64) -> Option<[u8; 16]> {
        self.topology
            .ordinal_of(node_id)
            .and_then(|ordinal| self.topology.uuids.get(ordinal).copied())
    }

    /// Resolve a public UUID to its internal node surrogate.
    ///
    /// The first lookups scan the UUID array; a caller that keeps looking pays
    /// once for a UUID-sorted permutation. Neither builds a hash map.
    #[must_use]
    pub(crate) fn node_id(&self, node_uuid: &[u8; 16]) -> Option<u64> {
        self.uuid_lookup
            .find(&self.topology.uuids, node_uuid)
            .map(|ordinal| self.topology.node_id_of(ordinal))
    }

    /// Exact graph-native label, loaded only for synchronous label propagation.
    pub(crate) fn initial_label(&self, node_id: u64) -> Option<i64> {
        self.node_initial_labels.get(&node_id).copied()
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
        if vectors.len() != self.node_count()
            || self
                .node_ids()
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
        self.node_count() == 0
    }

    /// Number of direction-expanded adjacency entries.
    pub(crate) fn edge_entry_count(&self) -> u64 {
        u64::try_from(self.topology.neighbors.len()).unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    pub(crate) fn with_test_counts(nodes: u64, edges: u64) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let uuids = node_ids
            .iter()
            .map(|&id| u128::from(id).to_be_bytes())
            .collect();
        let entries = if nodes == 0 {
            Vec::new()
        } else {
            (0..edges)
                .map(|edge_id| {
                    (
                        0,
                        AlgorithmEdge {
                            edge_id,
                            edge_uuid: u128::from(edge_id).to_be_bytes(),
                            neighbor_id: edge_id % nodes,
                            weight: 1.0,
                        },
                    )
                })
                .collect()
        };
        Self::from_entries(false, node_ids, uuids, entries)
    }

    #[cfg(test)]
    pub(crate) fn with_test_edges(nodes: u64, edges: &[(u64, u64)]) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let uuids = node_ids
            .iter()
            .map(|&id| u128::from(id).to_be_bytes())
            .collect();
        let entries = edges
            .iter()
            .enumerate()
            .map(|(edge_id, &(source, target))| {
                let edge_id = u64::try_from(edge_id).expect("test edge count fits u64");
                (
                    source,
                    AlgorithmEdge {
                        edge_id,
                        edge_uuid: u128::from(edge_id).to_be_bytes(),
                        neighbor_id: target,
                        weight: 1.0,
                    },
                )
            })
            .collect();
        Self::from_entries(false, node_ids, uuids, entries)
    }

    #[cfg(test)]
    pub(crate) fn with_test_directed_edges(nodes: u64, edges: &[(u64, u64)]) -> Self {
        let mut graph = Self::with_test_edges(nodes, edges);
        graph.topology.directed = true;
        graph
    }

    #[cfg(test)]
    pub(crate) fn with_test_directed_edges_and_uuids(
        node_uuids: &[[u8; 16]],
        edges: &[(u64, u64)],
    ) -> Self {
        let mut graph =
            Self::with_test_directed_edges(u64::try_from(node_uuids.len()).unwrap(), edges);
        graph.topology.uuids = node_uuids.to_vec();
        graph.uuid_lookup = UuidLookup::default();
        graph
    }

    #[cfg(test)]
    pub(crate) fn with_test_undirected_multigraph(nodes: u64, edges: &[(u64, u64, u64)]) -> Self {
        let node_ids: Vec<u64> = (0..nodes).collect();
        let uuids = node_ids
            .iter()
            .map(|&id| u128::from(id).to_be_bytes())
            .collect();
        let mut entries = Vec::new();
        for &(edge_id, source, target) in edges {
            let edge = |neighbor_id| AlgorithmEdge {
                edge_id,
                edge_uuid: u128::from(edge_id).to_be_bytes(),
                neighbor_id,
                weight: 1.0,
            };
            entries.push((source, edge(target)));
            if source != target {
                entries.push((target, edge(source)));
            }
        }
        Self::from_entries(false, node_ids, uuids, entries)
    }

    #[cfg(test)]
    pub(crate) fn with_test_edge_weights(mut self, weights: &[f64]) -> Self {
        let identity = self.topology.identity.as_ref().expect("fixture identity");
        let mut slots = (0..identity.edge_ids.len()).collect::<Vec<_>>();
        slots.sort_by_key(|&slot| identity.edge_ids[slot]);
        assert_eq!(slots.len(), weights.len());
        let mut assigned = vec![1.0; slots.len()];
        for (slot, &weight) in slots.into_iter().zip(weights) {
            assigned[slot] = weight;
        }
        self.topology.weights = Some(assigned);
        self
    }
}

/// Export only stable node selection and UUID identity, without reading edges.
pub(crate) fn export_node_selection(
    dir: &Path,
    label: EntityTypeSelection,
) -> Result<AdjacencyGraph, GfError> {
    export_node_selection_from_files(
        &graphforge_storage::TopologyFiles::discover_legacy(dir)?,
        label,
    )
}

pub(crate) fn export_node_selection_from_files(
    files: &graphforge_storage::TopologyFiles,
    label: EntityTypeSelection,
) -> Result<AdjacencyGraph, GfError> {
    // No edges follow, so no CSR bounds the rank table: lookups by id search.
    let selected = select_nodes(files, label, 0)?;
    let count = selected.nodes.len();
    Ok(AdjacencyGraph::from_topology(
        TopologyGraph {
            directed: false,
            offsets: vec![0; count + 1],
            neighbors: Vec::new(),
            weights: None,
            uuids: selected.uuids,
            nodes: selected.nodes,
            identity: None,
        },
        None,
        AlgorithmExportEvidence::default(),
    ))
}

/// Export the provider's adjacency into the Rust algorithm graph, keeping each
/// adjacency slot's edge id and UUID for kernels that report edges.
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
    export_topology(provider, dir, mode, selection, EdgeIdentityDemand::Retain)
}

/// Export the provider's adjacency as an ordinal topology (#1974).
///
/// The export is one sequential pass over the adjacency view's rows, in node
/// order, keeping the neighbors of each selected node that are themselves
/// selected. Nothing is sorted and no map is keyed by an edge or a node. The
/// edge table is read only to resolve edge UUIDs for [`EdgeIdentityDemand::Retain`]
/// or to look up a requested weight; a plain topology export reads none.
///
/// # Errors
/// As [`export_adjacency`].
pub(crate) fn export_topology(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    selection: AdjacencySelection<'_>,
    identity: EdgeIdentityDemand,
) -> Result<AdjacencyGraph, GfError> {
    let adjacency =
        crate::adjacency::AdjacencyReader::new(provider, selection.via, selection.direction)?;
    let inventory = provider.admitted_inventory();
    let files = match &inventory {
        Some(inventory) => graphforge_storage::TopologyFiles::from_inventory(inventory)?,
        None => graphforge_storage::TopologyFiles::discover_legacy(dir)?,
    };
    let selected = select_nodes(&files, selection.label, adjacency.node_extent())?;
    let node_count = selected.nodes.len();

    let keep_edge_ids = identity == EdgeIdentityDemand::Retain || selection.weight.is_some();
    let entry_hint = if node_count == 0 {
        0
    } else {
        adjacency.entry_count()?
    };
    let mut builder = TopologyBuilder::new(node_count, entry_hint, keep_edge_ids)?;
    if node_count > 0 {
        adjacency.walk_rows(|node_id, edge_ids, neighbor_ids| {
            let Some(ordinal) = selected.nodes.ordinal_of(node_id) else {
                return Ok(());
            };
            if ordinal != builder.next_ordinal() {
                return Err(GfError::Execution(format!(
                    "adjacency row {node_id} arrived out of node order"
                )));
            }
            builder.push_row(&selected.nodes, edge_ids, neighbor_ids)
        })?;
    }
    let (offsets, neighbors, edge_ids) = builder.finish(node_count);

    let mut evidence = AlgorithmExportEvidence {
        edge_scale_hash_entries: adjacency.hash_entries(),
        edge_rows_read: 0,
    };
    let mut retained = None;
    let mut weights = None;
    if let Some(edge_ids) = edge_ids {
        let (edge_uuids, rows) = resolve_edge_uuids(&files, selection.via, mode, &edge_ids)?;
        evidence.edge_rows_read = rows;
        if let Some(property) = selection.weight {
            weights = Some(resolve_weights(
                dir,
                inventory.as_deref(),
                selection.via,
                property,
                &edge_uuids,
            )?);
        }
        if identity == EdgeIdentityDemand::Retain {
            retained = Some(EdgeIdentity {
                edge_ids,
                edge_uuids,
            });
        }
    }
    Ok(AdjacencyGraph::from_topology(
        TopologyGraph {
            directed: !matches!(selection.direction, Direction::Undirected),
            offsets,
            neighbors,
            weights,
            uuids: selected.uuids,
            nodes: selected.nodes,
            identity: retained,
        },
        inventory,
        evidence,
    ))
}

/// Shape and evidence of one analyst export, for resource measurements that
/// live outside this crate (#1974).
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AnalystExportShape {
    /// Selected nodes.
    pub nodes: usize,
    /// Adjacency entries kept, both orientations of an undirected export.
    pub entries: usize,
    /// Whether a weight per entry was kept.
    pub weighted: bool,
    /// Whether the edge id and UUID of each entry were kept.
    pub identity: bool,
    /// Hash-map entries held by the adjacency view the export read.
    pub edge_scale_hash_entries: u64,
    /// Edge-table rows the export streamed.
    pub edge_rows_read: u64,
}

/// Run the real analyst export and report its shape. The graph is dropped on
/// return, so a caller measuring a heap peak around this call sees the export's
/// own peak.
///
/// # Errors
/// As the export: storage, execution and weight-validation errors.
#[doc(hidden)]
#[allow(
    clippy::too_many_arguments,
    reason = "one flat entry point for measurements outside this crate"
)]
pub fn export_analyst_topology(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    via: &str,
    direction: Direction,
    weight: Option<&str>,
    retain_identity: bool,
) -> Result<AnalystExportShape, GfError> {
    let graph = export_topology(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via,
            direction,
            weight,
        },
        if retain_identity {
            EdgeIdentityDemand::Retain
        } else {
            EdgeIdentityDemand::Omit
        },
    )?;
    let evidence = graph.export_evidence();
    Ok(AnalystExportShape {
        nodes: graph.node_count(),
        entries: graph.topology().neighbors.len(),
        weighted: graph.topology().weights.is_some(),
        identity: graph.topology().identity.is_some(),
        edge_scale_hash_entries: evidence.edge_scale_hash_entries,
        edge_rows_read: evidence.edge_rows_read,
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
    validate_vector_shape(graph.node_count(), 1)?;
    let mut values = HashMap::new();
    for stem in property_stems {
        for (uuid, row) in graph.node_property_rows(dir, stem).map_err(storage_error)? {
            if graph.node_id(&uuid).is_none() {
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
    let mut vectors = HashMap::with_capacity(graph.node_count());
    for &node_id in graph.node_ids() {
        let uuid = selected_uuid(graph, node_id)?;
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
            validate_vector_shape(graph.node_count(), vector.len())?;
        }
        vectors.insert(node_id, vector);
    }
    graph.replace_node_vectors(vectors)
}

/// Load exact integer labels without passing through a floating-point matrix.
pub(crate) fn load_node_initial_labels(
    graph: &mut AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<(), GfError> {
    let mut labels = HashMap::new();
    for stem in graph.property_routes(dir, false) {
        for (uuid, row) in graph
            .node_property_rows(dir, &stem)
            .map_err(storage_error)?
        {
            let Some(node_id) = graph.node_id(&uuid) else {
                continue;
            };
            let Some(value) = row.get(property) else {
                continue;
            };
            let IrLiteral::Int(label) = value else {
                return Err(GfError::Validation(format!(
                    "node {} initial label property {property:?} must be Int64",
                    uuid_text(&uuid)
                )));
            };
            if let Some(previous) = labels.insert(node_id, *label)
                && previous != *label
            {
                return Err(GfError::Validation(format!(
                    "node {} has conflicting initial label property {property:?}",
                    uuid_text(&uuid)
                )));
            }
        }
    }
    for &node in graph.node_ids() {
        if !labels.contains_key(&node) {
            return Err(GfError::Validation(format!(
                "node {} is missing initial label property {property:?}",
                uuid_text(&selected_uuid(graph, node)?)
            )));
        }
    }
    graph.node_initial_labels = labels;
    Ok(())
}

/// Load one strict graph-native numeric property for every selected node.
pub(crate) fn load_node_numeric_property(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<HashMap<u64, f64>, GfError> {
    let mut values = HashMap::new();
    for stem in graph.property_routes(dir, false) {
        for (uuid, row) in graph
            .node_property_rows(dir, &stem)
            .map_err(storage_error)?
        {
            let Some(node_id) = graph.node_id(&uuid) else {
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
                uuid_text(&selected_uuid(graph, node_id)?)
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
        .node_ids()
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
        .node_ids()
        .iter()
        .map(|&node_id| (node_id, Vec::new()))
        .collect::<HashMap<_, _>>();
    for property in properties {
        let mut values = HashMap::new();
        for stem in graph.property_routes(dir, false) {
            for (uuid, row) in graph
                .node_property_rows(dir, &stem)
                .map_err(storage_error)?
            {
                if graph.node_id(&uuid).is_none() {
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
        for &node_id in graph.node_ids() {
            let uuid = selected_uuid(graph, node_id)?;
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

/// UUID of a node the graph selected.
fn selected_uuid(graph: &AdjacencyGraph, node_id: u64) -> Result<[u8; 16], GfError> {
    graph
        .node_uuid(node_id)
        .ok_or_else(|| GfError::Execution("selected node has no UUID identity".into()))
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
    for stem in graph.property_routes(dir, false) {
        rows.extend(
            graph
                .node_property_rows(dir, &stem)
                .map_err(storage_error)?,
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

#[cfg(all(test, feature = "algorithms-extended"))]
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

    fn admitted_inventory(
        dir: &std::path::Path,
    ) -> std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory> {
        graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap()
            .admitted_inventory()
            .unwrap()
    }

    fn scan_provider(dir: std::path::PathBuf, mode: OntologyMode) -> ScanBuildAdjacencyProvider {
        let inventory = admitted_inventory(&dir);
        ScanBuildAdjacencyProvider::new(dir, mode).with_inventory(inventory)
    }

    fn persistent_provider(
        dir: std::path::PathBuf,
        mode: OntologyMode,
    ) -> PersistentAdjacencyProvider {
        let inventory = admitted_inventory(&dir);
        PersistentAdjacencyProvider::new(dir, mode).with_inventory(inventory)
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
                .create_node_with_labels(uuids[0], &[EntityTypeId::decode(1).unwrap()])
                .unwrap(),
            writer
                .create_node_with_labels(
                    uuids[1],
                    &[
                        EntityTypeId::decode(1).unwrap(),
                        EntityTypeId::decode(2).unwrap(),
                    ],
                )
                .unwrap(),
            writer
                .create_node_with_labels(uuids[2], &[EntityTypeId::decode(2).unwrap()])
                .unwrap(),
            writer
                .create_node_with_labels(uuids[3], &[EntityTypeId::decode(1).unwrap()])
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
                        ("score".to_owned(), IrLiteral::Float(index + 1.0)),
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
            label: EntityTypeSelection::All,
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);

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
            out.neighbors(fixture.ids[0]).get(0).unwrap().edge_uuid,
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            AdjacencySelection {
                label: EntityTypeSelection::Known(EntityTypeId::decode(1).unwrap()),
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
    fn mapped_algorithm_metadata_preserves_reserved_relation_and_weight() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let source = new_v7();
        let target = new_v7();
        let edge = new_v7();
        let label = EntityTypeId::decode(1).unwrap();
        let source_id = writer.create_node_with_labels(source, &[label]).unwrap();
        let target_id = writer.create_node_with_labels(target, &[label]).unwrap();
        let edge_id = writer.create_edge(edge, "CON", &source, &target).unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("CON"),
                HashMap::from([("cost".to_owned(), IrLiteral::Float(-2.5))]),
            )
            .unwrap();
        for node in [&source, &target] {
            writer
                .set_properties(
                    node,
                    Some("AUX"),
                    HashMap::from([
                        ("score".to_owned(), IrLiteral::Float(7.0)),
                        (
                            "features".to_owned(),
                            IrLiteral::List(vec![IrLiteral::Float(2.0), IrLiteral::Float(3.0)]),
                        ),
                    ]),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let catalog = graphforge_storage::GraphCatalog::open(
            dir.path(),
            None,
            &graphforge_ir::RuntimeCatalog::new(),
        )
        .unwrap();
        let inventory = catalog.admitted_inventory().unwrap();
        assert_eq!(
            inventory
                .routes(graphforge_storage::PropertyRouteKind::Edge)
                .collect::<Vec<_>>(),
            vec!["CON"]
        );
        let mut graph = export_node_selection(dir.path(), EntityTypeSelection::All).unwrap();
        graph.retain_inventory(Some(std::sync::Arc::clone(&inventory)));
        let routes = graph.property_routes(dir.path(), false);
        assert_eq!(routes, vec!["AUX"]);
        load_node_vectors(&mut graph, dir.path(), &routes, "features").unwrap();
        assert_eq!(graph.node_ids().len(), 2);
        for &node in graph.node_ids() {
            assert_eq!(graph.node_vector(node), Some([2.0, 3.0].as_slice()));
        }
        let scores = load_node_numeric_property(&graph, dir.path(), "score").unwrap();
        assert_eq!(scores.len(), 2);
        assert!(scores.values().all(|value| *value == 7.0));
        for relation in ["CON", "*"] {
            let provider = scan_provider(dir.path().to_path_buf(), OntologyMode::Strict);
            let graph = export_adjacency(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                AdjacencySelection {
                    via: relation,
                    weight: Some("cost"),
                    ..selection(Direction::Out)
                },
            )
            .unwrap();
            assert_eq!(
                graph.neighbors(source_id).to_vec(),
                vec![AlgorithmEdge {
                    edge_id,
                    edge_uuid: to_bytes(&edge),
                    neighbor_id: target_id,
                    weight: -2.5,
                }],
                "relation {relation}"
            );
        }
    }

    #[test]
    fn selected_weights_preserve_negative_values() {
        let fixture = fixture();
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        let provider = scan_provider(dir.path().to_path_buf(), OntologyMode::Strict);
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
        load_node_feature_properties(&mut graph, dir.path(), &["features".into()]).unwrap();
    }

    #[test]
    fn graph_native_vectors_load_by_uuid_in_topology_order() {
        let fixture = fixture();
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
    fn graph_native_loaders_reject_conflicting_stems_and_ragged_features() {
        let fixture = fixture();
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        graphforge_storage::set_node_properties(
            fixture.dir.path(),
            "Other",
            &HashMap::from([(
                to_bytes(&fixture.uuids[0]),
                HashMap::from([
                    (
                        "features".into(),
                        IrLiteral::List(vec![IrLiteral::Float(99.0)]),
                    ),
                    ("score".into(), IrLiteral::Float(99.0)),
                ]),
            )]),
        )
        .unwrap();

        // The mutation creates a new fixture snapshot; existing views stay pinned.
        graph.retain_inventory(Some(admitted_inventory(fixture.dir.path())));
        for error in [
            load_node_vectors(
                &mut graph,
                fixture.dir.path(),
                &["Person".into(), "Other".into()],
                "features",
            )
            .unwrap_err(),
            load_node_numeric_property(&graph, fixture.dir.path(), "score").unwrap_err(),
            load_node_feature_properties(&mut graph, fixture.dir.path(), &["features".into()])
                .unwrap_err(),
        ] {
            assert!(error.to_string().contains("conflicting"), "{error}");
        }

        let ragged = self::fixture();
        let provider = scan_provider(ragged.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut ragged_graph = export_adjacency(
            &provider,
            ragged.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        graphforge_storage::set_node_properties(
            ragged.dir.path(),
            "Person",
            &HashMap::from([(
                to_bytes(&ragged.uuids[1]),
                HashMap::from([(
                    "features".into(),
                    IrLiteral::List(vec![IrLiteral::Float(1.0)]),
                )]),
            )]),
        )
        .unwrap();
        ragged_graph.retain_inventory(Some(admitted_inventory(ragged.dir.path())));
        let error = load_node_feature_properties(
            &mut ragged_graph,
            ragged.dir.path(),
            &["features".into()],
        )
        .unwrap_err();
        assert!(error.to_string().contains("width"), "{error}");
    }

    #[test]
    fn vector_loader_ignores_property_rows_for_unselected_nodes() {
        let fixture = fixture();
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();
        graphforge_storage::set_node_properties(
            fixture.dir.path(),
            "Foreign",
            &HashMap::from([(
                to_bytes(&new_v7()),
                HashMap::from([(
                    "foreign".into(),
                    IrLiteral::List(vec![IrLiteral::Float(1.0)]),
                )]),
            )]),
        )
        .unwrap();
        let error = load_node_vectors(
            &mut graph,
            fixture.dir.path(),
            &["Foreign".into()],
            "foreign",
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing vector property"));
    }

    #[test]
    fn scalar_feature_loader_is_ordered_and_rejects_invalid_or_missing_values() {
        let fixture = fixture();
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
        let mut graph = export_adjacency(
            &provider,
            fixture.dir.path(),
            OntologyMode::Strict,
            selection(Direction::Out),
        )
        .unwrap();

        load_node_scalar_features(&mut graph, fixture.dir.path(), &["score".into()]).unwrap();
        for (index, &node_id) in fixture.ids.iter().enumerate() {
            let index = f64::from(u32::try_from(index).unwrap());
            assert_eq!(graph.node_vector(node_id), Some([index + 1.0].as_slice()));
        }
        assert!(matches!(
            load_node_numeric_property(&graph, fixture.dir.path(), "side"),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            load_node_numeric_property(&graph, fixture.dir.path(), "missing"),
            Err(GfError::Validation(_))
        ));

        load_node_scalar_features(&mut graph, fixture.dir.path(), &[]).unwrap();
        assert!(
            fixture
                .ids
                .iter()
                .all(|&node_id| graph.node_vector(node_id).is_none())
        );
    }

    #[test]
    fn graph_native_partition_property_loads_without_knowledge_storage() {
        let fixture = fixture();
        let mut graph =
            export_node_selection(fixture.dir.path(), EntityTypeSelection::All).unwrap();
        graph.retain_inventory(Some(admitted_inventory(fixture.dir.path())));
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
        let provider = scan_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        graph.retain_inventory(Some(admitted_inventory(fixture.dir.path())));
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
            writer.register_existing_node(uuid, id).unwrap();
        }
        writer
            .create_edge(extra, "KNOWS", &fixture.uuids[1], &fixture.uuids[0])
            .unwrap();
        writer.flush().unwrap();

        let provider = persistent_provider(fixture.dir.path().to_path_buf(), OntologyMode::Strict);
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
        let base = || {
            AdjacencyGraph::from_entries(
                true,
                vec![0, 1],
                vec![uuid0, uuid1],
                vec![(
                    0,
                    AlgorithmEdge {
                        edge_id: 0,
                        neighbor_id: 1,
                        edge_uuid,
                        weight: 2.5,
                    },
                )],
            )
        };

        let valid = base().projection_fingerprint().expect("valid projection");
        assert_ne!(valid.as_bytes(), &[0; 32]);

        let mut missing_target = base();
        missing_target.topology.neighbors[0] = 9;
        assert_eq!(
            missing_target
                .projection_fingerprint()
                .unwrap_err()
                .to_string(),
            "execution error: algorithm projection target has no UUID identity"
        );

        let mut non_finite = base();
        non_finite
            .topology
            .weights
            .as_mut()
            .expect("fixture weights")[0] = f64::NAN;
        assert_eq!(
            non_finite.projection_fingerprint().unwrap_err().to_string(),
            "execution error: algorithm projection edge weight is not finite"
        );

        let mut unmirrored = base();
        unmirrored.topology.directed = false;
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

    #[test]
    fn numeric_projection_accepts_every_supported_arrow_primitive_width() {
        use arrow::array::{
            Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
            UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        };

        let arrays: Vec<(Box<dyn Array>, f64)> = vec![
            (Box::new(Float64Array::from(vec![1.5])), 1.5),
            (Box::new(Float32Array::from(vec![2.5])), 2.5),
            (Box::new(Int64Array::from(vec![3])), 3.0),
            (Box::new(Int32Array::from(vec![4])), 4.0),
            (Box::new(Int16Array::from(vec![5])), 5.0),
            (Box::new(Int8Array::from(vec![6])), 6.0),
            (Box::new(UInt64Array::from(vec![7])), 7.0),
            (Box::new(UInt32Array::from(vec![8])), 8.0),
            (Box::new(UInt16Array::from(vec![9])), 9.0),
            (Box::new(UInt8Array::from(vec![10])), 10.0),
        ];
        for (array, expected) in arrays {
            assert_eq!(numeric_value(array.as_ref(), 0), Some(expected));
        }
        assert_eq!(numeric_value(&StringArray::from(vec!["nope"]), 0), None);
        assert_eq!(
            numeric_value(&Int64Array::from(vec![(1_i64 << 53) + 1]), 0),
            None
        );
        assert_eq!(
            numeric_value(&UInt64Array::from(vec![(1_u64 << 53) + 1]), 0),
            None
        );
    }
}
