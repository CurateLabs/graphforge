//! Node tables of the over-budget bulk build, on scratch (#1929).
//!
//! When the sorted node UUIDs, the endpoint index, the degrees and the CSR key
//! table do not fit the memory budget, nodes go through scratch the way edges
//! do. A node's rank is its position in global UUID order, so range partitions
//! of the node UUID space give every partition a rank base: the number of nodes
//! in the earlier partitions. Endpoint resolution is then a partition-local
//! join.
//!
//! 1. **Scatter.** Node records (UUID, label id) go into node-UUID range
//!    partitions. Oversized ranges refine by the same radix steps edges use.
//! 2. **Refs.** While edges scatter, every edge contributes three refs (source,
//!    target, and a probe of its own UUID) routed by UUID to the node leaf that
//!    can hold them.
//! 3. **Endpoints.** Each leaf is loaded, sorted and checked for duplicates,
//!    written as a node run, and its refs stream against it: a hit becomes a
//!    resolved record routed to the edge leaf of its edge, counts toward the
//!    node's exact degrees, and a probe hit or a miss is flagged. Degrees feed
//!    the CSR key partitioners in rank order, so no per-node table exists.
//! 4. **Join.** The edge pass joins a leaf's resolved records to its edges.
//! 5. **Sweep.** Nodes Parquet and the ordinal pushes stream from the runs.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rayon::prelude::*;

use super::budget::ScratchPlan;
use super::emit::LabelStats;
use super::install::Installer;
use super::ordered::{Ordered, run_ordered};
use super::property_rows::PropertyRows;
use super::scratch::{LeafRouter, Partitions, Scatter, Scratch};
use super::scratch_csr::{KeyPartitioner, KeyPartitionerBuilder};
use super::scratch_edges::{EdgeRecord, RelationCache, ScatteredEdges, SharedDictionary};
use super::scratch_ranges::{
    RangeSpec, UuidBounds, observe, partition_of, refine_partitions, uuid_splitters,
};
use super::tables::{
    Tasks, admit_batch, check_cancelled, claim_in_order, copy_uuids, short_source,
};
use super::{
    BulkSource, ConstructionChunkKind, EntityTypeId, GfError, GraphConstructionBudgets, node_batch,
    required_string, storage,
};

pub(super) const NODE_RECORD: usize = 20;
pub(super) const REF_RECORD: usize = 33;
pub(super) const RESOLVED_RECORD: usize = 37;
/// An identity probe is the edge's own UUID and nothing else: it only asks
/// whether that identity is a node's, so it carries no edge UUID and no role.
pub(super) const PROBE_RECORD: usize = 16;

/// Failpoints inside the passes that exist only on this route.
const DURING_SCATTER: &str = "bulk.during_node_scatter";
const DURING_REFINEMENT: &str = "bulk.during_node_refinement";
const DURING_RESOLVE: &str = "bulk.during_endpoint_resolve";
const DURING_EMIT: &str = "bulk.during_node_emit";

/// Node records are 20 bytes starting with the UUID. `row_bytes` is the plan's.
pub(super) const NODE_RANGES: RangeSpec = RangeSpec {
    width: NODE_RECORD,
    row_bytes: 0,
    what: "node",
    prefix: "node",
    failpoint: DURING_REFINEMENT,
};

/// A node before it has a rank.
#[derive(Clone, Copy)]
pub(super) struct NodeRecord {
    pub(super) uuid: [u8; 16],
    pub(super) label: u32,
}

impl NodeRecord {
    fn encode(&self) -> [u8; NODE_RECORD] {
        let mut bytes = [0_u8; NODE_RECORD];
        bytes[..16].copy_from_slice(&self.uuid);
        bytes[16..].copy_from_slice(&self.label.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            uuid: bytes[..16].try_into().expect("16 bytes"),
            label: u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes")),
        }
    }
}

pub(super) const ROLE_SRC: u8 = 0;
pub(super) const ROLE_DST: u8 = 1;

/// A UUID an edge asks the node leaves about: one of its endpoints.
#[derive(Clone, Copy)]
pub(super) struct RefRecord {
    pub(super) key: [u8; 16],
    pub(super) edge: [u8; 16],
    pub(super) role: u8,
}

impl RefRecord {
    fn encode(&self) -> [u8; REF_RECORD] {
        let mut bytes = [0_u8; REF_RECORD];
        bytes[..16].copy_from_slice(&self.key);
        bytes[16..32].copy_from_slice(&self.edge);
        bytes[32] = self.role;
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            key: bytes[..16].try_into().expect("16 bytes"),
            edge: bytes[16..32].try_into().expect("16 bytes"),
            role: bytes[32],
        }
    }
}

/// An endpoint with its rank, ready to join to its edge.
#[derive(Clone, Copy)]
struct ResolvedRecord {
    edge: [u8; 16],
    endpoint: [u8; 16],
    role: u8,
    rank: u32,
}

impl ResolvedRecord {
    fn encode(&self) -> [u8; RESOLVED_RECORD] {
        let mut bytes = [0_u8; RESOLVED_RECORD];
        bytes[..16].copy_from_slice(&self.edge);
        bytes[16..32].copy_from_slice(&self.endpoint);
        bytes[32] = self.role;
        bytes[33..].copy_from_slice(&self.rank.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            edge: bytes[..16].try_into().expect("16 bytes"),
            endpoint: bytes[16..32].try_into().expect("16 bytes"),
            role: bytes[32],
            rank: u32::from_le_bytes(bytes[33..].try_into().expect("4 bytes")),
        }
    }
}

/// The UUIDs of an edge's endpoints, in the order of the edge's records.
#[derive(Clone, Copy)]
pub(super) struct EndpointPair {
    pub(super) src: [u8; 16],
    pub(super) dst: [u8; 16],
}

// ------------------------------------------------------------------ pass 1

/// The nodes after the scatter: range partitions, unsorted within each.
pub(super) struct ScatteredNodes {
    pub(super) leaves: Partitions,
    pub(super) router: LeafRouter,
    pub(super) counts: Vec<u64>,
    pub(super) total: u64,
    pub(super) label_names: Vec<String>,
    pub(super) refinement_steps: u64,
    pub(super) refinement_write_bytes: u64,
    pub(super) refinement_read_bytes: u64,
}

/// Decode every node once and scatter its record into a node-UUID range
/// partition.
#[allow(clippy::too_many_lines)]
pub(super) fn scatter_nodes(
    sources: &[BulkSource<'_>],
    budgets: GraphConstructionBudgets,
    properties: Option<&PropertyRows<'_>>,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<ScatteredNodes, GfError> {
    let tasks = Tasks::plan(sources, "nodes")?;
    let splitters = uuid_splitters(sources, &tasks, plan.node_partitions, "node_uuid", cancel)?;
    let partitions = Partitions::create(scratch, "nodes", splitters.len() + 1, NODE_RECORD)?;
    let staging = plan.staging_for(partitions.len());
    let bounds = (0..partitions.len())
        .map(|_| Mutex::new(None::<([u8; 16], [u8; 16])>))
        .collect::<Vec<_>>();
    let dictionary = SharedDictionary::default();
    claim_in_order(tasks.items.clone(), |(source, task, rows)| {
        check_cancelled(cancel)?;
        let mut scatter = Scatter::new(scratch, &partitions, staging);
        let mut cache = RelationCache::new(&dictionary);
        let mut written = 0;
        let mut uuids = Vec::new();
        let mut labels = Vec::new();
        let mut task_bounds = vec![None; partitions.len()];
        sources[source].reader.read_task(task, &mut |batch| {
            check_cancelled(cancel)?;
            if !sources[source].reader.admitted() {
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Node,
                    &batch,
                )?;
                admit_batch(ConstructionChunkKind::Node, &batch, budgets)?;
            }
            let count = batch.num_rows();
            if written + count > rows {
                return Err(short_source());
            }
            if count == 0 {
                return Ok(());
            }
            uuids.clear();
            uuids.resize(count, [0_u8; 16]);
            copy_uuids(
                crate::graph_construction::batch_uuid_column(&batch, "node_uuid")?,
                &mut uuids,
            );
            labels.clear();
            labels.resize(count, 0);
            cache.column(required_string(&batch, "label")?, &mut labels)?;
            for (uuid, label) in uuids.iter().zip(&labels) {
                let part = partition_of(&splitters, uuid);
                observe(&mut task_bounds[part], *uuid);
                scatter.push(
                    part,
                    &NodeRecord {
                        uuid: *uuid,
                        label: *label,
                    }
                    .encode(),
                )?;
            }
            if let Some(properties) = properties {
                properties.ingest(&batch, cancel)?;
            }
            written += count;
            crate::graph_construction::construction_failpoint(DURING_SCATTER);
            Ok(())
        })?;
        if written != rows {
            return Err(short_source());
        }
        scatter.finish()?;
        for (part, task_bounds) in task_bounds.into_iter().enumerate() {
            if let Some((low, high)) = task_bounds {
                let mut shared = bounds[part]
                    .lock()
                    .map_err(|_| storage("UUID bounds lock poisoned"))?;
                observe(&mut shared, low);
                observe(&mut shared, high);
            }
        }
        Ok(())
    })?;
    let total = partitions.counts()?.iter().sum::<u64>();
    if total != tasks.total as u64 {
        return Err(short_source());
    }
    let bounds = bounds
        .into_iter()
        .map(|bounds| {
            bounds
                .into_inner()
                .map_err(|_| storage("UUID bounds lock poisoned"))
        })
        .collect::<Result<Vec<UuidBounds>, _>>()?;
    let refined = refine_partitions(
        &partitions,
        &bounds,
        RangeSpec {
            row_bytes: plan.node_row_bytes,
            ..NODE_RANGES
        },
        plan,
        scratch,
        cancel,
    )?;
    let counts = refined.partitions.counts()?;
    Ok(ScatteredNodes {
        router: LeafRouter::new(&refined.lows),
        leaves: refined.partitions,
        counts,
        total,
        label_names: dictionary.into_names()?,
        refinement_steps: refined.steps,
        refinement_write_bytes: refined.write_bytes,
        refinement_read_bytes: refined.read_bytes,
    })
}

// ------------------------------------------------------------------ pass 2

/// Where the edge pass sends the refs and the identity probes of its edges.
pub(super) struct RefSink<'a> {
    pub(super) router: &'a LeafRouter,
    pub(super) refs: &'a Partitions,
    /// The identity probes: one 16-byte edge UUID each, routed to the same
    /// node leaves as the refs.
    pub(super) probes: &'a Partitions,
}

impl RefSink<'_> {
    /// Stage `record` for the node leaf that can hold its key. Returns `false`
    /// when there is no node leaf at all, so the key cannot be a node.
    pub(super) fn push(
        &self,
        scatter: &mut Scatter<'_>,
        record: &RefRecord,
    ) -> Result<bool, GfError> {
        let Some(leaf) = self.router.route(&record.key) else {
            return Ok(false);
        };
        scatter.push(leaf, &record.encode())?;
        Ok(true)
    }

    /// Stage the identity probe of one edge for the node leaf that can hold
    /// it. Without a node leaf nothing can collide, so there is nothing to do.
    pub(super) fn push_probe(
        &self,
        probes: &mut Scatter<'_>,
        edge: &[u8; 16],
    ) -> Result<(), GfError> {
        if let Some(leaf) = self.router.route(edge) {
            probes.push(leaf, edge)?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ pass 3

/// What the endpoint pass learned.
pub(super) struct ResolvedNodes {
    /// The sorted nodes, one run per leaf, in rank order.
    pub(super) runs: Partitions,
    /// Resolved endpoints, one file per edge leaf.
    pub(super) resolved: Partitions,
    pub(super) labels: LabelStats,
    /// The smallest endpoint no node answered to.
    pub(super) miss: Option<[u8; 16]>,
    /// Some edge UUID is a node UUID.
    pub(super) collision: bool,
}

pub(super) struct ResolveContext<'a> {
    pub(super) scratch: &'a Scratch,
    pub(super) plan: &'a ScratchPlan,
    pub(super) nodes: &'a ScatteredNodes,
    pub(super) refs: &'a Partitions,
    pub(super) probes: &'a Partitions,
    pub(super) edges: &'a ScatteredEdges,
    pub(super) cancel: &'a AtomicBool,
}

/// Rank every node leaf and resolve the refs routed to it. Also returns the
/// out and in CSR key partitioners the exact degrees give.
#[allow(clippy::too_many_lines)]
pub(super) fn resolve_endpoints(
    context: &ResolveContext<'_>,
) -> Result<(ResolvedNodes, (KeyPartitioner, KeyPartitioner)), GfError> {
    let ResolveContext {
        scratch,
        plan,
        nodes,
        refs,
        probes,
        edges,
        cancel,
    } = *context;
    let leaves = nodes.leaves.len();
    let mut bases = Vec::with_capacity(leaves);
    let mut running = 0_u64;
    for count in &nodes.counts {
        bases.push(running);
        running += count;
    }
    let label_count = nodes.label_names.len();
    let first_rank = (0..label_count)
        .map(|_| AtomicU64::new(u64::MAX))
        .collect::<Vec<_>>();
    let label_counts = (0..label_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    let collision = AtomicBool::new(false);
    let miss = Mutex::new(None::<[u8; 16]>);
    let builders = Mutex::new((
        KeyPartitionerBuilder::new(plan.csr_entry_limit(), edges.total, plan.csr_partitions),
        KeyPartitionerBuilder::new(plan.csr_entry_limit(), edges.total, plan.csr_partitions),
    ));
    let edge_router = LeafRouter::new(&edges.lows);
    let resolved =
        Partitions::create(scratch, "resolved", edges.partitions.len(), RESOLVED_RECORD)?;
    let runs = Partitions::create(scratch, "node-runs", leaves, NODE_RECORD)?;
    let staging = plan.staging_for(edges.partitions.len());
    let ordered = Ordered::new(plan.gate_bytes, 1, cancel);
    run_ordered(
        leaves,
        plan.concurrency,
        &ordered,
        |leaf| plan.node_cost(nodes.counts[leaf]),
        |leaf| {
            let base = bases[leaf];
            let count = usize::try_from(nodes.counts[leaf]).map_err(storage)?;
            let mut records = Vec::with_capacity(count);
            nodes.leaves.read(scratch, leaf, |payload| {
                if !payload.len().is_multiple_of(NODE_RECORD) {
                    return Err(storage("a node scratch block has a partial record"));
                }
                records.extend(payload.chunks_exact(NODE_RECORD).map(NodeRecord::decode));
                Ok(())
            })?;
            if records.len() != count {
                return Err(storage("a node scratch partition lost records"));
            }
            // The raw leaf ends here: the sorted run below is what the sweeps
            // read. Its file is reclaimed once this read has verified it.
            nodes.leaves.reclaim(scratch, leaf)?;
            records.sort_unstable_by_key(|record| record.uuid);
            if records.windows(2).any(|pair| pair[0].uuid == pair[1].uuid) {
                return Err(storage(
                    "duplicate identity across construction runs (node)",
                ));
            }
            {
                let mut run = Scatter::new(scratch, &runs, 256 << 10);
                for record in &records {
                    run.push(leaf, &record.encode())?;
                }
                run.finish()?;
            }
            // Label statistics in rank order.
            let mut first = vec![u64::MAX; label_count];
            let mut seen = vec![0_u64; label_count];
            for (position, record) in records.iter().enumerate() {
                let label = record.label as usize;
                if first[label] == u64::MAX {
                    first[label] = position as u64;
                }
                seen[label] += 1;
            }
            for label in 0..label_count {
                if seen[label] != 0 {
                    first_rank[label].fetch_min(base + first[label] + 1, Ordering::Relaxed);
                    label_counts[label].fetch_add(seen[label], Ordering::Relaxed);
                }
            }
            // The identity probes of this leaf: an edge UUID that is a node
            // UUID. A probe only binary-searches its own edge UUID here, so it
            // is a bare 16-byte record; the collision flag outlives the file,
            // which is reclaimed once this read has verified it.
            let mut local_collision = false;
            probes.read(scratch, leaf, |payload| {
                if !payload.len().is_multiple_of(PROBE_RECORD) {
                    return Err(storage("a node probe block has a partial record"));
                }
                for bytes in payload.chunks_exact(PROBE_RECORD) {
                    let edge: [u8; 16] = bytes.try_into().expect("16 bytes");
                    if records
                        .binary_search_by(|record| record.uuid.cmp(&edge))
                        .is_ok()
                    {
                        local_collision = true;
                    }
                }
                Ok(())
            })?;
            probes.reclaim(scratch, leaf)?;
            // The refs of this leaf, streamed against its sorted records.
            let mut out_degrees = vec![0_u32; count];
            let mut in_degrees = vec![0_u32; count];
            let mut scatter = Scatter::new(scratch, &resolved, staging);
            let mut local_miss = None::<[u8; 16]>;
            refs.read(scratch, leaf, |payload| {
                check_cancelled(cancel)?;
                if !payload.len().is_multiple_of(REF_RECORD) {
                    return Err(storage("a node reference block has a partial record"));
                }
                for bytes in payload.chunks_exact(REF_RECORD) {
                    let reference = RefRecord::decode(bytes);
                    match records.binary_search_by(|record| record.uuid.cmp(&reference.key)) {
                        Ok(position) => match reference.role {
                            role @ (ROLE_SRC | ROLE_DST) => {
                                let degrees = if role == ROLE_SRC {
                                    &mut out_degrees
                                } else {
                                    &mut in_degrees
                                };
                                degrees[position] += 1;
                                let target =
                                    edge_router.route(&reference.edge).ok_or_else(|| {
                                        storage("an endpoint reference lost its edge partition")
                                    })?;
                                scatter.push(
                                    target,
                                    &ResolvedRecord {
                                        edge: reference.edge,
                                        endpoint: reference.key,
                                        role,
                                        rank: u32::try_from(base + position as u64 + 1)
                                            .map_err(storage)?,
                                    }
                                    .encode(),
                                )?;
                            }
                            _ => return Err(storage("a node reference has an unknown role")),
                        },
                        Err(_) => {
                            local_miss =
                                Some(local_miss.map_or(reference.key, |m| m.min(reference.key)));
                        }
                    }
                }
                crate::graph_construction::construction_failpoint(DURING_RESOLVE);
                Ok(())
            })?;
            // Every reference this leaf will ever see is resolved; the file is
            // not read again.
            refs.reclaim(scratch, leaf)?;
            scatter.finish()?;
            if local_collision {
                collision.store(true, Ordering::Relaxed);
            }
            if let Some(endpoint) = local_miss {
                let mut shared = miss.lock().map_err(|_| storage("endpoint lock poisoned"))?;
                *shared = Some(shared.map_or(endpoint, |m| m.min(endpoint)));
            }
            // Degrees reach the key partitioners in rank order.
            #[cfg(test)]
            std::thread::sleep(std::time::Duration::from_millis(
                plan.stagger_millis * (leaves - leaf) as u64,
            ));
            ordered.wait_turn(0, leaf)?;
            {
                let mut builders = builders
                    .lock()
                    .map_err(|_| storage("key partitioner lock poisoned"))?;
                builders.0.extend(&out_degrees);
                builders.1.extend(&in_degrees);
            }
            ordered.pass_turn(0);
            Ok(())
        },
    )?;
    check_cancelled(cancel)?;
    let (out_builder, in_builder) = builders
        .into_inner()
        .map_err(|_| storage("key partitioner lock poisoned"))?;
    let counts = label_counts
        .iter()
        .map(|count| count.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let mut first_appearance = (0..label_count)
        .filter(|label| counts[*label] != 0)
        .map(|label| u32::try_from(label).expect("bounded label dictionary"))
        .collect::<Vec<_>>();
    first_appearance
        .sort_unstable_by_key(|label| first_rank[*label as usize].load(Ordering::Relaxed));
    Ok((
        ResolvedNodes {
            runs,
            resolved,
            labels: LabelStats {
                names: nodes.label_names.clone(),
                first_appearance,
                counts,
            },
            miss: miss
                .into_inner()
                .map_err(|_| storage("endpoint lock poisoned"))?,
            collision: collision.load(Ordering::Relaxed),
        },
        (out_builder.finish(), in_builder.finish()),
    ))
}

// ------------------------------------------------------------------ pass 4

/// Fill in the endpoint ranks of one edge leaf's sorted `records` from its
/// resolved records, and return the endpoints' UUIDs.
pub(super) fn join_endpoints(
    scratch: &Scratch,
    resolved: &Partitions,
    leaf: usize,
    records: &mut [EdgeRecord],
) -> Result<Vec<EndpointPair>, GfError> {
    let mut joined = Vec::with_capacity(records.len() * 2);
    resolved.read(scratch, leaf, |payload| {
        if !payload.len().is_multiple_of(RESOLVED_RECORD) {
            return Err(storage("an endpoint scratch block has a partial record"));
        }
        joined.extend(
            payload
                .chunks_exact(RESOLVED_RECORD)
                .map(ResolvedRecord::decode),
        );
        Ok(())
    })?;
    if joined.len() != records.len() * 2 {
        return Err(storage("an edge scratch partition lost endpoints"));
    }
    // The join inputs are in the records now; their files are not read again.
    resolved.reclaim(scratch, leaf)?;
    joined.sort_unstable_by_key(|record| (record.edge, record.role));
    let mut pairs = Vec::with_capacity(records.len());
    for (record, ends) in records.iter_mut().zip(joined.chunks_exact(2)) {
        let (src, dst) = (&ends[0], &ends[1]);
        if src.role != ROLE_SRC
            || dst.role != ROLE_DST
            || src.edge != record.uuid
            || dst.edge != record.uuid
        {
            return Err(storage("an edge scratch partition lost endpoints"));
        }
        record.src = src.rank;
        record.dst = dst.rank;
        pairs.push(EndpointPair {
            src: src.endpoint,
            dst: dst.endpoint,
        });
    }
    Ok(pairs)
}

// ------------------------------------------------------------------ pass 5

/// What the node sweep needs.
pub(super) struct NodeSweep<'a> {
    pub(super) scratch: &'a Scratch,
    pub(super) runs: &'a Partitions,
    pub(super) installer: &'a Installer<'a>,
    /// The entity type of every label id.
    pub(super) types: &'a [EntityTypeId],
    pub(super) window: usize,
    pub(super) now: i64,
    pub(super) cancel: &'a AtomicBool,
}

struct NodeWindow {
    first_id: u64,
    uuids: Vec<[u8; 16]>,
    types: Vec<EntityTypeId>,
}

/// Stream the node runs in rank order: write the canonical node files, and
/// hand every `(uuid, rank)` to `each` (the ordinal index). Returns the nodes
/// seen.
pub(super) fn sweep_nodes(
    sweep: &NodeSweep<'_>,
    mut each: impl FnMut(&[u8; 16], u64) -> Result<(), GfError>,
) -> Result<u64, GfError> {
    let NodeSweep {
        scratch,
        runs,
        installer,
        types,
        window,
        now,
        cancel,
    } = *sweep;
    let batch_windows = (rayon::current_num_threads() * 2).clamp(2, 16);
    let mut pending = Vec::<NodeWindow>::new();
    let mut current = NodeWindow {
        first_id: 1,
        uuids: Vec::new(),
        types: Vec::new(),
    };
    let mut rank = 0_u64;
    let flush = |pending: &mut Vec<NodeWindow>| -> Result<(), GfError> {
        std::mem::take(pending)
            .into_par_iter()
            .try_for_each(|window| {
                check_cancelled(cancel)?;
                let last = window.first_id + window.uuids.len() as u64 - 1;
                let ids = (window.first_id..=last).collect::<Vec<_>>();
                let batch = node_batch(&window.uuids, &ids, &window.types, now)?;
                installer.install_parquet(
                    &format!(
                        "topology/nodes/{:020}-{:020}.parquet",
                        window.first_id, last
                    ),
                    &batch,
                )
            })
    };
    for leaf in 0..runs.len() {
        runs.read(scratch, leaf, |payload| {
            check_cancelled(cancel)?;
            if !payload.len().is_multiple_of(NODE_RECORD) {
                return Err(storage("a node run has a partial record"));
            }
            for bytes in payload.chunks_exact(NODE_RECORD) {
                let record = NodeRecord::decode(bytes);
                rank += 1;
                each(&record.uuid, rank)?;
                current.uuids.push(record.uuid);
                current.types.push(
                    *types
                        .get(record.label as usize)
                        .ok_or_else(|| storage("node label is absent from runtime catalog"))?,
                );
                if current.uuids.len() == window {
                    let first_id = rank + 1;
                    pending.push(std::mem::replace(
                        &mut current,
                        NodeWindow {
                            first_id,
                            uuids: Vec::new(),
                            types: Vec::new(),
                        },
                    ));
                    if pending.len() >= batch_windows {
                        flush(&mut pending)?;
                    }
                }
            }
            crate::graph_construction::construction_failpoint(DURING_EMIT);
            Ok(())
        })?;
        // This leaf's run fed the canonical files and the ordinal pushes; it
        // is not read again.
        runs.reclaim(scratch, leaf)?;
    }
    if !current.uuids.is_empty() {
        pending.push(current);
    }
    flush(&mut pending)?;
    Ok(rank)
}

#[cfg(test)]
mod tests {
    use super::super::scratch_edges::{EDGE_RECORD, ScatteredEdges};
    use super::*;
    use crate::graph_construction_encoding::StableDirectory;

    /// An identity that grows with `last`, so fixtures can order and split.
    fn monoton(first: u8, last: u8) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[0] = first;
        value[15] = last;
        value
    }

    fn resolve_plan() -> ScratchPlan {
        ScratchPlan::sized(1, 1, 1, 1 << 20, 4096)
    }

    /// Two node leaves — `a`, `b` in the first, `c`, `d` in the second, in
    /// arrival order — beside the empty ref and probe leaves the edge pass
    /// fills and the edge side the join consumes.
    fn fixture(
        scratch: &Scratch,
    ) -> Result<(ScatteredNodes, Partitions, Partitions, ScatteredEdges), GfError> {
        let (a, b, c, d) = (
            monoton(0x10, 1),
            monoton(0x10, 2),
            monoton(0x20, 1),
            monoton(0x20, 2),
        );
        let leaves = Partitions::create(scratch, "nodes", 2, NODE_RECORD)?;
        let mut scatter = Scatter::new(scratch, &leaves, 256 << 10);
        for (leaf, uuids) in [(0, [a, b].as_slice()), (1, [c, d].as_slice())] {
            for uuid in uuids {
                scatter.push(
                    leaf,
                    &NodeRecord {
                        uuid: *uuid,
                        label: 0,
                    }
                    .encode(),
                )?;
            }
        }
        scatter.finish()?;
        let lows = vec![Some(a), Some(c)];
        let scattered = ScatteredNodes {
            router: LeafRouter::new(&lows),
            counts: vec![2, 2],
            total: 4,
            label_names: vec!["Person".to_owned()],
            leaves,
            refinement_steps: 0,
            refinement_write_bytes: 0,
            refinement_read_bytes: 0,
        };
        let refs = Partitions::create(scratch, "refs", 2, REF_RECORD)?;
        let probes = Partitions::create(scratch, "probes", 2, PROBE_RECORD)?;
        let edges = ScatteredEdges {
            partitions: Partitions::create(scratch, "edges", 1, EDGE_RECORD)?,
            lows: vec![Some(monoton(0x30, 1))],
            refinement_write_bytes: 0,
            refinement_read_bytes: 0,
            refinement_steps: 0,
            counts: vec![0],
            rel_names: vec!["KNOWS".to_owned()],
            histogram: None,
            total: 0,
        };
        Ok((scattered, refs, probes, edges))
    }

    fn sink_of<'a>(
        nodes: &'a ScatteredNodes,
        refs: &'a Partitions,
        probes: &'a Partitions,
    ) -> RefSink<'a> {
        RefSink {
            router: &nodes.router,
            refs,
            probes,
        }
    }

    fn context<'a>(
        scratch: &'a Scratch,
        plan: &'a ScratchPlan,
        nodes: &'a ScatteredNodes,
        refs: &'a Partitions,
        probes: &'a Partitions,
        edges: &'a ScatteredEdges,
        cancel: &'a AtomicBool,
    ) -> ResolveContext<'a> {
        ResolveContext {
            scratch,
            plan,
            nodes,
            refs,
            probes,
            edges,
            cancel,
        }
    }

    #[test]
    fn a_probe_whose_edge_uuid_is_a_node_uuid_sets_the_collision() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
        let sink = sink_of(&nodes, &refs, &probes);
        let mut scatter = Scatter::new(&scratch, &probes, 256 << 10);
        for edge in [monoton(0x10, 2), monoton(0x30, 1), monoton(0x30, 2)] {
            // The first probe is node b's UUID; the others are pure edges.
            sink.push_probe(&mut scatter, &edge).unwrap();
        }
        scatter.finish().unwrap();
        let cancel = AtomicBool::new(false);
        let plan = resolve_plan();
        let (resolved, _) = resolve_endpoints(&context(
            &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
        ))
        .unwrap();
        assert!(resolved.collision, "a probe of a node UUID must collide");
        assert!(resolved.miss.is_none());
        // Every file the pass consumed is reclaimed, collisions included.
        for leaf in 0..2 {
            assert!(!nodes.leaves.path(leaf).exists());
            assert!(!refs.path(leaf).exists());
            assert!(!probes.path(leaf).exists());
        }
        scratch.remove().unwrap();
    }

    #[test]
    fn endpoint_refs_resolve_alongside_compact_probes_without_nodes_in_them() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
        let sink = sink_of(&nodes, &refs, &probes);
        // The edge's own UUID probes its leaf, and no node holds it.
        let mut probe_scatter = Scatter::new(&scratch, &probes, 256 << 10);
        sink.push_probe(&mut probe_scatter, &monoton(0x30, 1))
            .unwrap();
        probe_scatter.finish().unwrap();
        // One edge with both endpoints in the first leaf.
        let (a, b, e) = (monoton(0x10, 1), monoton(0x10, 2), monoton(0x30, 1));
        let mut refs_scatter = Scatter::new(&scratch, &refs, 256 << 10);
        for (role, key) in [(ROLE_SRC, a), (ROLE_DST, b)] {
            assert!(
                sink.push(&mut refs_scatter, &RefRecord { key, edge: e, role })
                    .unwrap()
            );
        }
        refs_scatter.finish().unwrap();
        let cancel = AtomicBool::new(false);
        let plan = resolve_plan();
        let (resolved, _) = resolve_endpoints(&context(
            &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
        ))
        .unwrap();
        assert!(!resolved.collision, "edge UUIDs must not collide");
        assert!(resolved.miss.is_none());
        let mut joined = Vec::new();
        resolved
            .resolved
            .read(&scratch, 0, |payload| {
                joined.extend(
                    payload
                        .chunks_exact(RESOLVED_RECORD)
                        .map(ResolvedRecord::decode),
                );
                Ok(())
            })
            .unwrap();
        joined.sort_unstable_by_key(|record| (record.edge, record.role));
        assert_eq!(joined.len(), 2);
        assert_eq!(
            (joined[0].role, joined[0].endpoint, joined[0].rank),
            (ROLE_SRC, a, 1)
        );
        assert_eq!(
            (joined[1].role, joined[1].endpoint, joined[1].rank),
            (ROLE_DST, b, 2)
        );
        // Probes and refs are spent; the runs and the resolved records await
        // their own consumers.
        assert!(!probes.path(0).exists());
        assert!(!refs.path(0).exists());
        assert!(resolved.runs.path(0).exists());
        assert!(resolved.resolved.path(0).exists());
        scratch.remove().unwrap();
    }

    #[test]
    fn a_probe_file_is_deleted_only_after_a_verified_read() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
        let sink = sink_of(&nodes, &refs, &probes);
        let mut scatter = Scatter::new(&scratch, &probes, 256 << 10);
        sink.push_probe(&mut scatter, &monoton(0x30, 1)).unwrap();
        scatter.finish().unwrap();
        // The probe routes to the leaf that can hold it: the second one.
        let path = probes.path(1);
        let mut bytes = std::fs::read(path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let error = {
            let cancel = AtomicBool::new(false);
            let plan = resolve_plan();
            resolve_endpoints(&context(
                &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
            ))
            .map(|_| ())
            .unwrap_err()
        };
        assert!(error.to_string().contains("CRC32C"), "{error}");
        assert!(path.exists(), "an unverified probe file is not reclaimed");
    }
}
