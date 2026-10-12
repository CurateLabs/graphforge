//! Hash-map-free topology of an analyst export (#1974).
//!
//! The export of a persisted adjacency is one sequential pass over its CSR rows.
//! Every selected node gets an *ordinal* in ascending `node_id` order, and the
//! topology keeps only `u32` ordinals: an offsets array and a flat neighbor
//! array. UUIDs, edge identity and weights are parallel arrays indexed by
//! ordinal or by adjacency slot, filled by sorted merges against the sorted
//! node and edge tables. No map is keyed by an edge or by a node anywhere in
//! the build.

use std::ops::Range;

use arrow::array::Array;
use graphforge_core::{GfError, OntologyMode};
use graphforge_storage::{DenseNodeIdentityError, TopologyFiles};
use graphforge_value::{EntityTypeId, EntityTypeSelection};

use super::{fixed_binary, numeric_value, uuid_at, uuid_text};

/// Ordinal reported by a rank table for a node that is not selected.
const NO_ORDINAL: u32 = u32::MAX;

/// Which nodes an export selected and how a `node_id` maps to its ordinal.
#[derive(Clone, Debug)]
pub(crate) enum NodeSet {
    /// Consecutive ids: the node with ordinal `n` has `node_id = first + n`.
    Dense { first: u64, count: usize },
    /// Ascending ids with gaps (deleted nodes) or a label subset.
    Sparse {
        /// `ids[n]` is the `node_id` of ordinal `n`.
        ids: Vec<u64>,
        /// `rank[id]` is the ordinal of `node_id = id`, or [`NO_ORDINAL`], for
        /// every `id` below `rank.len()`. Ids at or above it are found in `ids`.
        rank: Vec<u32>,
    },
}

impl Default for NodeSet {
    fn default() -> Self {
        Self::Dense { first: 0, count: 0 }
    }
}

impl NodeSet {
    /// A set over ascending `ids`, dense when they are consecutive. `rank_cap`
    /// bounds the rank table; ids beyond it are found by binary search.
    pub(crate) fn from_ids(ids: Vec<u64>, rank_cap: u64) -> Self {
        let Some(&first) = ids.first() else {
            return Self::default();
        };
        let consecutive = ids
            .iter()
            .zip(first..)
            .all(|(&id, expected)| id == expected);
        if consecutive {
            return Self::Dense {
                first,
                count: ids.len(),
            };
        }
        let last = ids.last().copied().unwrap_or(0);
        let length = usize::try_from(last.saturating_add(1).min(rank_cap)).unwrap_or(usize::MAX);
        let mut rank = vec![NO_ORDINAL; length];
        for (ordinal, &id) in ids.iter().enumerate() {
            if let Some(slot) = usize::try_from(id).ok().and_then(|id| rank.get_mut(id)) {
                *slot = u32::try_from(ordinal).unwrap_or(NO_ORDINAL);
            }
        }
        Self::Sparse { ids, rank }
    }

    /// Selected nodes.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Dense { count, .. } => *count,
            Self::Sparse { ids, .. } => ids.len(),
        }
    }

    /// Ordinal of `node_id`, if selected.
    pub(crate) fn ordinal_of(&self, node_id: u64) -> Option<usize> {
        match self {
            Self::Dense { first, count } => {
                let ordinal = usize::try_from(node_id.checked_sub(*first)?).ok()?;
                (ordinal < *count).then_some(ordinal)
            }
            Self::Sparse { ids, rank } => match usize::try_from(node_id) {
                Ok(index) if index < rank.len() => {
                    let ordinal = rank[index];
                    (ordinal != NO_ORDINAL).then_some(ordinal as usize)
                }
                _ => ids.binary_search(&node_id).ok(),
            },
        }
    }

    /// `node_id` of `ordinal`. An ordinal outside the selection resolves to an
    /// id no selected node owns, so a malformed neighbor stays unresolvable.
    pub(crate) fn node_id_of(&self, ordinal: usize) -> u64 {
        match self {
            Self::Dense { first, count } if ordinal < *count => {
                first.saturating_add(ordinal as u64)
            }
            Self::Dense { .. } => u64::MAX,
            Self::Sparse { ids, .. } => ids.get(ordinal).copied().unwrap_or(u64::MAX),
        }
    }
}

/// Edge identity per adjacency slot, kept only for kernels that report edges.
#[derive(Clone, Debug, Default)]
pub(crate) struct EdgeIdentity {
    /// Execution-internal edge surrogate of each slot.
    pub(crate) edge_ids: Vec<u64>,
    /// Stable public edge UUID of each slot.
    pub(crate) edge_uuids: Vec<[u8; 16]>,
}

/// CSR over ordinals: the whole structural content of an analyst export.
#[derive(Clone, Debug)]
pub(crate) struct TopologyGraph {
    pub(crate) directed: bool,
    /// `offsets[n]..offsets[n + 1]` is the slot range of ordinal `n`.
    pub(crate) offsets: Vec<u32>,
    /// Neighbor ordinal of each slot. An undirected view holds both
    /// orientations of an edge, merged by ascending `edge_id` with the out entry
    /// first on a tie.
    pub(crate) neighbors: Vec<u32>,
    /// Weight of each slot, present only when a weight property was requested.
    pub(crate) weights: Option<Vec<f64>>,
    /// Public UUID of each ordinal.
    pub(crate) uuids: Vec<[u8; 16]>,
    pub(crate) nodes: NodeSet,
    pub(crate) identity: Option<EdgeIdentity>,
}

impl Default for TopologyGraph {
    fn default() -> Self {
        Self {
            directed: false,
            offsets: vec![0],
            neighbors: Vec::new(),
            weights: None,
            uuids: Vec::new(),
            nodes: NodeSet::default(),
            identity: None,
        }
    }
}

impl TopologyGraph {
    /// Selected nodes.
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Ordinal of `node_id`, if selected.
    pub(crate) fn ordinal_of(&self, node_id: u64) -> Option<usize> {
        self.nodes.ordinal_of(node_id)
    }

    /// `node_id` of `ordinal`; see [`NodeSet::node_id_of`].
    pub(crate) fn node_id_of(&self, ordinal: usize) -> u64 {
        self.nodes.node_id_of(ordinal)
    }

    /// Slot range of `ordinal`; empty when the ordinal is outside the graph.
    pub(crate) fn slots(&self, ordinal: usize) -> Range<usize> {
        match (self.offsets.get(ordinal), self.offsets.get(ordinal + 1)) {
            (Some(&start), Some(&end)) => {
                let end = (end as usize).min(self.neighbors.len());
                (start as usize).min(end)..end
            }
            _ => 0..0,
        }
    }

    pub(crate) fn neighbor_ordinals(&self, ordinal: usize) -> &[u32] {
        &self.neighbors[self.slots(ordinal)]
    }

    pub(crate) fn neighbor_weights(&self, ordinal: usize) -> Option<&[f64]> {
        self.weights
            .as_ref()
            .map(|weights| &weights[self.slots(ordinal)])
    }

    /// Bytes held by the structural arrays, by their allocated capacity.
    pub(crate) fn retained_bytes(&self) -> usize {
        let node_set = match &self.nodes {
            NodeSet::Dense { .. } => 0,
            NodeSet::Sparse { ids, rank } => 8 * ids.capacity() + 4 * rank.capacity(),
        };
        let identity = self.identity.as_ref().map_or(0, |identity| {
            8 * identity.edge_ids.capacity() + 16 * identity.edge_uuids.capacity()
        });
        4 * self.offsets.capacity()
            + 4 * self.neighbors.capacity()
            + self.weights.as_ref().map_or(0, |w| 8 * w.capacity())
            + 16 * self.uuids.capacity()
            + node_set
            + identity
    }
}

/// Nodes an export selected, with their UUIDs by ordinal.
pub(crate) struct SelectedNodes {
    pub(crate) nodes: NodeSet,
    pub(crate) uuids: Vec<[u8; 16]>,
}

/// Select nodes in ascending `node_id` order from a sorted node-table scan.
///
/// An unfiltered selection of a table with consecutive ids is read as one
/// two-column projection and proved dense. Anything else (a label subset, or ids
/// with gaps after deletes) streams the same projection with label membership
/// and builds a rank table bounded by `rank_cap`.
pub(crate) fn select_nodes(
    files: &TopologyFiles,
    label: EntityTypeSelection,
    rank_cap: u64,
) -> Result<SelectedNodes, GfError> {
    let known = match label {
        EntityTypeSelection::Missing => {
            return Ok(SelectedNodes {
                nodes: NodeSet::default(),
                uuids: Vec::new(),
            });
        }
        EntityTypeSelection::All => match graphforge_storage::read_dense_node_identities(files) {
            Ok(dense) => {
                return Ok(SelectedNodes {
                    nodes: NodeSet::Dense {
                        first: dense.first_node_id,
                        count: dense.uuids.len(),
                    },
                    uuids: dense.uuids,
                });
            }
            Err(DenseNodeIdentityError::NotDense { .. }) => None,
            Err(DenseNodeIdentityError::Read(error)) => return Err(error),
        },
        EntityTypeSelection::Known(label) => Some(label),
    };
    stream_nodes(files, known, rank_cap)
}

fn stream_nodes(
    files: &TopologyFiles,
    label: Option<EntityTypeId>,
    rank_cap: u64,
) -> Result<SelectedNodes, GfError> {
    let mut ids = Vec::new();
    let mut uuids = Vec::new();
    graphforge_storage::visit_node_identities(files, label, |id, uuid| {
        ids.push(id);
        uuids.push(*uuid);
        Ok(())
    })?;
    ids.shrink_to_fit();
    uuids.shrink_to_fit();
    Ok(SelectedNodes {
        nodes: NodeSet::from_ids(ids, rank_cap),
        uuids,
    })
}

/// Sequential builder of the offsets and neighbor arrays.
pub(crate) struct TopologyBuilder {
    offsets: Vec<u32>,
    neighbors: Vec<u32>,
    /// `edge_id` of each slot, kept while edge identity or weights are needed.
    edge_ids: Option<Vec<u64>>,
    /// Entries of a row that arrived out of order, sorted before they are kept.
    unsorted: Vec<(u64, u64)>,
}

impl TopologyBuilder {
    pub(crate) fn new(
        node_count: usize,
        entry_hint: u64,
        keep_edge_ids: bool,
    ) -> Result<Self, GfError> {
        let entries = usize::try_from(entry_hint)
            .map_err(|_| size_error("adjacency entry count exceeds the address space"))?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(node_count + 1)
            .map_err(|_| size_error("analyst export offsets allocation failed"))?;
        offsets.push(0);
        let mut neighbors = Vec::new();
        neighbors
            .try_reserve_exact(entries)
            .map_err(|_| size_error("analyst export neighbors allocation failed"))?;
        let edge_ids = if keep_edge_ids {
            let mut ids = Vec::new();
            ids.try_reserve_exact(entries)
                .map_err(|_| size_error("analyst export edge identity allocation failed"))?;
            Some(ids)
        } else {
            None
        };
        Ok(Self {
            offsets,
            neighbors,
            edge_ids,
            unsorted: Vec::new(),
        })
    }

    /// Ordinal of the next row to be pushed.
    pub(crate) fn next_ordinal(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Keep the neighbors of one selected node that are themselves selected.
    ///
    /// A row arrives in CSR order, which is ascending `(edge_id, neighbor_id)`.
    /// A row that is not (a corrupt writer) is sorted rather than trusted, so
    /// the topology always holds the order the algorithms were specified on.
    pub(crate) fn push_row(
        &mut self,
        nodes: &NodeSet,
        edge_ids: &[u64],
        neighbor_ids: &[u64],
    ) -> Result<(), GfError> {
        let len = edge_ids.len().min(neighbor_ids.len());
        let (edge_ids, neighbor_ids) = (&edge_ids[..len], &neighbor_ids[..len]);
        let ordered = edge_ids
            .windows(2)
            .zip(neighbor_ids.windows(2))
            .all(|(edges, neighbors)| (edges[0], neighbors[0]) <= (edges[1], neighbors[1]));
        if ordered {
            for (&edge_id, &neighbor_id) in edge_ids.iter().zip(neighbor_ids) {
                self.keep(nodes, edge_id, neighbor_id)?;
            }
        } else {
            let mut row = std::mem::take(&mut self.unsorted);
            row.clear();
            row.extend(edge_ids.iter().copied().zip(neighbor_ids.iter().copied()));
            row.sort_unstable();
            for &(edge_id, neighbor_id) in &row {
                self.keep(nodes, edge_id, neighbor_id)?;
            }
            self.unsorted = row;
        }
        let end = u32::try_from(self.neighbors.len())
            .map_err(|_| size_error("analyst export exceeds 2^32 - 1 adjacency entries"))?;
        self.offsets.push(end);
        Ok(())
    }

    fn keep(&mut self, nodes: &NodeSet, edge_id: u64, neighbor_id: u64) -> Result<(), GfError> {
        let ordinal = nodes.ordinal_of(neighbor_id);
        let Some(ordinal) = ordinal else {
            return Ok(());
        };
        self.neighbors.push(
            u32::try_from(ordinal)
                .map_err(|_| size_error("analyst export exceeds 2^32 - 1 nodes"))?,
        );
        if let Some(edge_ids) = &mut self.edge_ids {
            edge_ids.push(edge_id);
        }
        Ok(())
    }

    /// Close the offsets over nodes that no CSR row covered (ids past the
    /// index's extent have no entries) and release unused capacity.
    pub(crate) fn finish(mut self, node_count: usize) -> (Vec<u32>, Vec<u32>, Option<Vec<u64>>) {
        let end = self.offsets.last().copied().unwrap_or(0);
        self.offsets.resize(node_count + 1, end);
        self.neighbors.shrink_to_fit();
        if let Some(edge_ids) = &mut self.edge_ids {
            edge_ids.shrink_to_fit();
        }
        (self.offsets, self.neighbors, self.edge_ids)
    }
}

fn size_error(message: &str) -> GfError {
    GfError::Execution(message.into())
}

/// Slots ordered by the key `key(slot)`, as a `u32` permutation.
fn slot_order<K: Ord>(slots: usize, key: impl Fn(usize) -> K) -> Result<Vec<u32>, GfError> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(slots)
        .map_err(|_| size_error("analyst export slot order allocation failed"))?;
    for slot in 0..slots {
        order.push(
            u32::try_from(slot).map_err(|_| size_error("analyst export exceeds 2^32 - 1 slots"))?,
        );
    }
    order.sort_unstable_by_key(|&slot| key(slot as usize));
    Ok(order)
}

/// Forward-moving lookup into a key-sorted slot permutation.
///
/// Rows of a sorted table are almost always visited in ascending key order, so
/// the cursor advances linearly. A key below the previous one (the next file of
/// a union, or an unsorted table) repositions it by binary search instead of
/// assuming order.
struct SortedCursor<K> {
    position: usize,
    previous: Option<K>,
}

impl<K: Ord + Copy> SortedCursor<K> {
    fn new() -> Self {
        Self {
            position: 0,
            previous: None,
        }
    }

    /// Range of `order` whose slots have key `wanted`.
    fn find(&mut self, order: &[u32], key: impl Fn(u32) -> K, wanted: K) -> Range<usize> {
        if self.previous.is_some_and(|previous| wanted < previous) {
            self.position = order.partition_point(|&slot| key(slot) < wanted);
        } else {
            while self.position < order.len() && key(order[self.position]) < wanted {
                self.position += 1;
            }
        }
        self.previous = Some(wanted);
        let mut end = self.position;
        while end < order.len() && key(order[end]) == wanted {
            end += 1;
        }
        self.position..end
    }
}

/// Edge UUID of every adjacency slot, by a sorted merge of the slots' edge ids
/// against the edge table's `(edge_id, edge_uuid)` projection.
///
/// Returns the UUIDs and the number of edge-table rows read.
pub(crate) fn resolve_edge_uuids(
    files: &TopologyFiles,
    via: &str,
    mode: OntologyMode,
    edge_ids: &[u64],
) -> Result<(Vec<[u8; 16]>, u64), GfError> {
    let slots = edge_ids.len();
    if slots == 0 {
        return Ok((Vec::new(), 0));
    }
    let order = slot_order(slots, |slot| edge_ids[slot])?;
    let mut uuids = Vec::new();
    uuids
        .try_reserve_exact(slots)
        .map_err(|_| size_error("analyst export edge UUID allocation failed"))?;
    uuids.resize(slots, [0; 16]);
    let mut filled = vec![0_u64; slots.div_ceil(64)];
    let mut cursor = SortedCursor::new();
    let mut rows = 0_u64;
    graphforge_storage::visit_edge_identities(files, via, mode, |ids, edge_uuid_column| {
        rows += ids.len() as u64;
        for row in 0..ids.len() {
            let edge_id = ids.value(row);
            let matches = cursor.find(&order, |slot| edge_ids[slot as usize], edge_id);
            if matches.is_empty() {
                continue;
            }
            let uuid = uuid_at(edge_uuid_column, row)?;
            for &slot in &order[matches] {
                uuids[slot as usize] = uuid;
                filled[slot as usize / 64] |= 1 << (slot % 64);
            }
        }
        Ok(())
    })?;
    if let Some(slot) = (0..slots).find(|&slot| filled[slot / 64] & (1 << (slot % 64)) == 0) {
        return Err(GfError::Execution(format!(
            "adjacency edge {} has no topology UUID",
            edge_ids[slot]
        )));
    }
    Ok((uuids, rows))
}

/// Weight of every adjacency slot, by a sorted merge of the slots' edge UUIDs
/// against the requested property of each edge-property route.
///
/// A requested weight is strict: every slot must end up with one finite numeric
/// value. Negative finite weights are kept; individual algorithms accept or
/// reject them.
pub(crate) fn resolve_weights(
    dir: &std::path::Path,
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    via: &str,
    property: &str,
    edge_uuids: &[[u8; 16]],
) -> Result<Vec<f64>, GfError> {
    if edge_uuids.is_empty() {
        return Ok(Vec::new());
    }
    let order = slot_order(edge_uuids.len(), |slot| edge_uuids[slot])?;
    let mut weights = Vec::new();
    weights
        .try_reserve_exact(edge_uuids.len())
        .map_err(|_| size_error("analyst export weight allocation failed"))?;
    weights.resize(edge_uuids.len(), f64::NAN);
    let stems: Vec<String> = if via == "*" {
        match inventory {
            Some(inventory) => inventory
                .routes(graphforge_storage::PropertyRouteKind::Edge)
                .map(str::to_owned)
                .collect(),
            None => graphforge_storage::list_edge_property_stems(dir),
        }
    } else {
        vec![via.to_owned()]
    };
    let requested = [property.to_owned()];
    for stem in stems {
        let mut cursor = SortedCursor::new();
        graphforge_storage::visit_edge_property_batches(
            dir,
            inventory,
            &stem,
            &requested,
            |batch| {
                let uuids = fixed_binary(batch, "edge_uuid")?;
                let values = batch.column_by_name(property).ok_or_else(|| {
                    GfError::Validation(format!("edge weight property {property:?} does not exist"))
                })?;
                for row in 0..batch.num_rows() {
                    if uuids.is_null(row) {
                        continue;
                    }
                    let uuid = uuid_at(uuids, row)?;
                    let matches = cursor.find(&order, |slot| edge_uuids[slot as usize], uuid);
                    if matches.is_empty() || values.is_null(row) {
                        continue;
                    }
                    let weight = numeric_value(values.as_ref(), row).ok_or_else(|| {
                        GfError::Validation(format!(
                            "edge weight property {property:?} must be numeric"
                        ))
                    })?;
                    for &slot in &order[matches] {
                        weights[slot as usize] = weight;
                    }
                }
                Ok(())
            },
        )?;
    }
    for (slot, weight) in weights.iter().enumerate() {
        if !weight.is_finite() {
            return Err(GfError::Validation(format!(
                "edge weight is missing, NULL, NaN, or infinite for edge {}",
                uuid_text(&edge_uuids[slot])
            )));
        }
    }
    Ok(weights)
}
