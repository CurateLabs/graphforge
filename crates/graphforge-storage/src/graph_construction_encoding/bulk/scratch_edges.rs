//! Passes 2 and 3 of the over-budget bulk build (#1900).
//!
//! Pass 2 decodes the edges once and scatters a compact record (UUID, source
//! and target rank, relation id) into edge-UUID range partitions on scratch.
//! The range boundaries come from a sample of the edge UUIDs. When the node
//! tables are resident the endpoints resolve here, through the node index.
//! When the node tables are on scratch too, the record carries no ranks: pass
//! 2 then writes only the raw records and hashes a canonical topology proof
//! per task, and a second pass over the same planned tasks sends the
//! endpoints to the node leaves (#1929) only after the raw partitions have
//! refined — so the refinement never overlaps live reference files — and only
//! after its replay proof equals pass 2's (#1929 phase order). The node
//! leaves' resolved records pass 3 joins back.
//!
//! Pass 3 builds the partitions in order, several at a time within the memory
//! gate. A partition holds every edge of its UUID range, so sorting it ranks
//! its edges: the first edge's `edge_id` is the number of edges in the earlier
//! partitions plus one. Each partition checks its identities, writes the
//! canonical edge files, stages its adjacency entries into node-range
//! partitions, and keeps its sorted UUIDs for the membership index.
//!
//! Canonical edge files cover fixed windows of `edge_id`s, and a window can
//! straddle two partitions. The rows after the last whole window carry to the
//! next partition, one turn at a time in partition order; encoding happens
//! outside the turn.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::StringArray;
use sha2::Digest as _;

use super::budget::ScratchPlan;
use super::emit::{EdgeEmitter, EdgeWindow};
use super::ordered::{Ordered, run_ordered};
use super::scratch::{Partitions, Scatter, Scratch};
use super::scratch_csr::{CsrRecord, CsrScratch, KeyHistogram};
use super::scratch_nodes::{EndpointPair, ROLE_DST, ROLE_SRC, RefRecord, RefSink, join_endpoints};
use super::scratch_ranges::{
    RangeSpec, UuidBounds, observe, partition_of, refine_partitions, uuid_splitters,
};
use super::tables::{
    NodeIndex, NodeTable, Tasks, admit_batch, check_cancelled, claim_in_order, copy_uuids,
    short_source,
};
use super::{
    BulkSource, ConstructionChunkKind, GfError, GraphConstructionBudgets, Sha256, required_string,
    storage,
};

// ----------------------------------------------------------------- proof

/// Domain tag of the deferred route's canonical edge topology proof. It keeps
/// these digests from ever colliding with any other SHA-256 use, and the
/// domain-separated task identities keep two planned tasks — even two
/// identical row streams — from producing equal task hashes that would cancel
/// in the XOR accumulator.
const EDGE_TOPOLOGY_DOMAIN: &[u8] = b"graphforge.bulk.edge-topology.v1";

/// The canonical topology hash of one planned task: the domain-separated
/// source index, task index and exact expected row count, then the ordered
/// stream of edge UUID, source UUID, target UUID, and length-prefixed
/// relation-name bytes, one tuple per row in task order. The hash runs
/// continuously across the batches a task emits, so a changed batch boundary
/// cannot change it; only the row stream can.
struct TaskTopology(Sha256);

impl TaskTopology {
    fn start(source: usize, task: usize, rows: usize) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(EDGE_TOPOLOGY_DOMAIN);
        hasher.update(u64::try_from(source).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(u64::try_from(task).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(u64::try_from(rows).unwrap_or(u64::MAX).to_le_bytes());
        Self(hasher)
    }

    fn row(&mut self, edge: &[u8; 16], source: &[u8; 16], target: &[u8; 16], relation: &str) {
        let hasher = &mut self.0;
        hasher.update(edge);
        hasher.update(source);
        hasher.update(target);
        hasher.update(
            u64::try_from(relation.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(relation.as_bytes());
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

/// The constant-space aggregate of one pass's task hashes: a fixed 32-byte
/// XOR accumulator under one mutex, combined independently of the order the
/// workers finished in. Every task's exact row count must match its footer
/// plan before its hash may enter; the fixed 256-bit result is the pass's
/// deterministic cryptographic replay evidence, not a source-property replay
/// claim.
struct TopologyProof {
    accumulator: Mutex<[u8; 32]>,
}

impl TopologyProof {
    fn new() -> Self {
        Self {
            accumulator: Mutex::new([0_u8; 32]),
        }
    }

    fn add_task(&self, task: [u8; 32]) -> Result<(), GfError> {
        let mut accumulator = self
            .accumulator
            .lock()
            .map_err(|_| storage("edge topology proof lock poisoned"))?;
        for (slot, byte) in accumulator.iter_mut().zip(task) {
            *slot ^= byte;
        }
        Ok(())
    }

    fn finish(self) -> Result<[u8; 32], GfError> {
        self.accumulator
            .into_inner()
            .map_err(|_| storage("edge topology proof lock poisoned"))
    }
}

pub(super) const EDGE_RECORD: usize = 28;

/// Edge records are 28 bytes starting with the UUID. `row_bytes` is the plan's.
pub(super) const EDGE_RANGES: RangeSpec = RangeSpec {
    width: EDGE_RECORD,
    row_bytes: 44,
    what: "edge",
    prefix: "edge",
    failpoint: "bulk.during_edge_refinement",
};

/// A resolved edge before it has a rank.
#[derive(Clone, Copy)]
pub(super) struct EdgeRecord {
    pub(super) uuid: [u8; 16],
    pub(super) src: u32,
    pub(super) dst: u32,
    pub(super) rel: u32,
}

impl EdgeRecord {
    pub(super) fn encode(&self) -> [u8; EDGE_RECORD] {
        let mut bytes = [0_u8; EDGE_RECORD];
        bytes[..16].copy_from_slice(&self.uuid);
        bytes[16..20].copy_from_slice(&self.src.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.dst.to_le_bytes());
        bytes[24..].copy_from_slice(&self.rel.to_le_bytes());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Self {
        let word = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
        Self {
            uuid: bytes[..16].try_into().expect("16 bytes"),
            src: word(16),
            dst: word(20),
            rel: word(24),
        }
    }
}

// ------------------------------------------------------------ dictionary

/// Names shared by every task, so a record carries its final id. Ids are in
/// first-seen order, which varies between runs; no output depends on it,
/// because every consumer orders by name or by the sorted rows.
#[derive(Default)]
pub(super) struct SharedDictionary {
    inner: Mutex<(Vec<String>, HashMap<String, u32>)>,
}

impl SharedDictionary {
    fn id(&self, name: &str) -> Result<u32, GfError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| storage("relation dictionary lock poisoned"))?;
        if let Some(id) = inner.1.get(name) {
            return Ok(*id);
        }
        let id = u32::try_from(inner.0.len()).map_err(storage)?;
        inner.0.push(name.to_owned());
        inner.1.insert(name.to_owned(), id);
        Ok(id)
    }

    pub(super) fn into_names(self) -> Result<Vec<String>, GfError> {
        self.inner
            .into_inner()
            .map(|inner| inner.0)
            .map_err(|_| storage("relation dictionary lock poisoned"))
    }
}

/// One task's view of the shared dictionary: names it has already resolved.
pub(super) struct RelationCache<'a> {
    shared: &'a SharedDictionary,
    ids: HashMap<String, u32>,
}

impl<'a> RelationCache<'a> {
    pub(super) fn new(shared: &'a SharedDictionary) -> Self {
        Self {
            shared,
            ids: HashMap::new(),
        }
    }

    pub(super) fn column(&mut self, values: &StringArray, out: &mut [u32]) -> Result<(), GfError> {
        let mut previous: Option<(&str, u32)> = None;
        for (row, slot) in out.iter_mut().enumerate() {
            let value = values.value(row);
            let id = match previous {
                Some((text, id)) if text == value => id,
                _ => {
                    if let Some(id) = self.ids.get(value) {
                        *id
                    } else {
                        let id = self.shared.id(value)?;
                        self.ids.insert(value.to_owned(), id);
                        id
                    }
                }
            };
            previous = Some((value, id));
            *slot = id;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ pass 2

/// The edges after pass 2: scattered, unranked.
pub(super) struct ScatteredEdges {
    pub(super) partitions: Partitions,
    /// The smallest UUID of each partition, `None` when it is empty. It routes
    /// the endpoints the node leaves resolve to the partition of their edge.
    pub(super) lows: Vec<Option<[u8; 16]>>,
    pub(super) refinement_write_bytes: u64,
    pub(super) refinement_read_bytes: u64,
    pub(super) refinement_steps: u64,
    pub(super) counts: Vec<u64>,
    pub(super) rel_names: Vec<String>,
    /// Exact degrees, when the endpoints resolved during the scatter.
    pub(super) histogram: Option<KeyHistogram>,
    pub(super) total: u64,
    /// The canonical topology proof the deferred route's first pass hashed;
    /// the reference pass must replay it exactly, and the resident route,
    /// which resolves in one pass, keeps none.
    pub(super) topology_proof: Option<[u8; 32]>,
}

fn missing_endpoint_error(is_edge: bool) -> GfError {
    storage(if is_edge {
        "edge endpoint is not a node UUID"
    } else {
        "edge endpoint UUID does not exist"
    })
}

/// Where the edge pass finds the ranks of the endpoints.
pub(super) enum Endpoints<'a> {
    /// The node tables are resident: resolve now, through the node index.
    Resident {
        nodes: &'a NodeTable,
        index: &'a NodeIndex<'a>,
    },
    /// The node tables are on scratch: the ranks stay zero, and a second pass
    /// over the same planned tasks sends the endpoints to the node leaves
    /// only after the raw partitions have refined and its replay proof has
    /// matched this pass's (#1929).
    Deferred,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn scatter_edges(
    sources: &[BulkSource<'_>],
    budgets: GraphConstructionBudgets,
    properties: Option<&super::property_rows::PropertyRows<'_>>,
    decode: &super::gate::ByteGate,
    endpoints: &Endpoints<'_>,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<ScatteredEdges, GfError> {
    let tasks = Tasks::plan(sources, "edges")?;
    let splitters = uuid_splitters(
        sources,
        &tasks,
        plan.edge_partitions,
        "edge_uuid",
        decode,
        cancel,
    )?;
    let partitions = Partitions::create(scratch, "edges", splitters.len() + 1, EDGE_RECORD)?;
    let staging = plan.staging_bytes;
    let bounds = (0..partitions.len())
        .map(|_| Mutex::new(None::<([u8; 16], [u8; 16])>))
        .collect::<Vec<_>>();
    let dictionary = SharedDictionary::default();
    let histogram = match endpoints {
        Endpoints::Resident { nodes, .. } => Some(KeyHistogram::new(
            nodes.uuids.len() as u64,
            tasks.total as u64,
            plan.csr_entry_limit(),
        )),
        Endpoints::Deferred => None,
    };
    // The deferred route hashes the canonical topology while the required
    // identity and endpoint columns are in hand; the reference pass must
    // reproduce it exactly.
    let proof = match endpoints {
        Endpoints::Deferred => Some(TopologyProof::new()),
        Endpoints::Resident { .. } => None,
    };
    let miss = Mutex::new(None::<[u8; 16]>);
    claim_in_order(tasks.items.clone(), |(source, task, rows)| {
        check_cancelled(cancel)?;
        let _decoding = decode.hold(sources[source].task_decode_bytes(task), cancel)?;
        let mut scatter = Scatter::new(scratch, &partitions, staging);
        let mut cache = RelationCache::new(&dictionary);
        let mut hasher = proof
            .as_ref()
            .map(|_| TaskTopology::start(source, task, rows));
        let mut written = 0;
        let mut uuids = Vec::new();
        let mut sources_ranks = Vec::new();
        let mut targets = Vec::new();
        let mut rels = Vec::new();
        let mut endpoint_uuids = Vec::new();
        let mut source_uuids = Vec::new();
        let mut target_uuids = Vec::new();
        let mut task_miss = None::<[u8; 16]>;
        let mut task_bounds = vec![None; partitions.len()];
        let mut sink = properties.map(super::property_rows::PropertyRows::sink);
        sources[source].reader.read_task(task, &mut |batch| {
            check_cancelled(cancel)?;
            if !sources[source].reader.admitted() {
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Edge,
                    &batch,
                )?;
                admit_batch(ConstructionChunkKind::Edge, &batch, budgets)?;
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
                crate::graph_construction::batch_uuid_column(&batch, "edge_uuid")?,
                &mut uuids,
            );
            rels.clear();
            rels.resize(count, 0);
            let relations = required_string(&batch, "rel_type")?;
            cache.column(relations, &mut rels)?;
            match endpoints {
                Endpoints::Resident { index, .. } => {
                    for (name, ranks) in [
                        ("source_uuid", &mut sources_ranks),
                        ("target_uuid", &mut targets),
                    ] {
                        endpoint_uuids.clear();
                        endpoint_uuids.resize(count, [0_u8; 16]);
                        copy_uuids(
                            crate::graph_construction::batch_uuid_column(&batch, name)?,
                            &mut endpoint_uuids,
                        );
                        ranks.clear();
                        ranks.extend(endpoint_uuids.iter().map(|endpoint| {
                            index.find(endpoint).unwrap_or_else(|| {
                                task_miss.get_or_insert(*endpoint);
                                0
                            })
                        }));
                    }
                }
                Endpoints::Deferred => {
                    for (name, column) in [
                        ("source_uuid", &mut source_uuids),
                        ("target_uuid", &mut target_uuids),
                    ] {
                        column.clear();
                        column.resize(count, [0_u8; 16]);
                        copy_uuids(
                            crate::graph_construction::batch_uuid_column(&batch, name)?,
                            column,
                        );
                    }
                }
            }
            for row in 0..count {
                let (src, dst) = match endpoints {
                    Endpoints::Resident { .. } => (sources_ranks[row], targets[row]),
                    Endpoints::Deferred => (0, 0),
                };
                let record = EdgeRecord {
                    uuid: uuids[row],
                    src,
                    dst,
                    rel: rels[row],
                };
                if let Some(histogram) = &histogram
                    && src != 0
                    && dst != 0
                {
                    histogram.add(src, dst);
                }
                let part = partition_of(&splitters, &record.uuid);
                observe(&mut task_bounds[part], record.uuid);
                scatter.push(part, &record.encode())?;
                if let Some(hasher) = &mut hasher {
                    hasher.row(
                        &record.uuid,
                        &source_uuids[row],
                        &target_uuids[row],
                        relations.value(row),
                    );
                }
            }
            if let Some(sink) = &mut sink {
                sink.push(&batch, cancel)?;
            }
            written += count;
            crate::graph_construction::construction_failpoint("bulk.during_edge_scatter");
            Ok(())
        })?;
        if written != rows {
            return Err(short_source());
        }
        if let Some(sink) = sink {
            sink.finish(cancel)?;
        }
        scatter.finish()?;
        // The task's exact row count has matched the footer plan; its proof
        // may enter the accumulator.
        if let (Some(proof), Some(hasher)) = (&proof, hasher) {
            proof.add_task(hasher.finish())?;
        }
        for (part, bounds_of_task) in task_bounds.into_iter().enumerate() {
            if let Some((low, high)) = bounds_of_task {
                let mut shared = bounds[part]
                    .lock()
                    .map_err(|_| storage("UUID bounds lock poisoned"))?;
                observe(&mut shared, low);
                observe(&mut shared, high);
            }
        }
        if let Some(endpoint) = task_miss {
            miss.lock()
                .map_err(|_| storage("endpoint lock poisoned"))?
                .get_or_insert(endpoint);
        }
        Ok(())
    })?;
    let initial_counts = partitions.counts()?;
    let total = initial_counts.iter().sum::<u64>();
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
            row_bytes: plan.edge_row_bytes,
            ..EDGE_RANGES
        },
        plan,
        scratch,
        cancel,
    )?;
    let counts = refined.partitions.counts()?;
    let topology_proof = match proof {
        Some(proof) => Some(proof.finish()?),
        None => None,
    };
    let scattered = ScatteredEdges {
        partitions: refined.partitions,
        lows: refined.lows,
        refinement_write_bytes: refined.write_bytes,
        refinement_read_bytes: refined.read_bytes,
        refinement_steps: refined.steps,
        counts,
        rel_names: dictionary.into_names()?,
        histogram,
        total,
        topology_proof,
    };
    // The in-memory build checks identities (a repeated edge UUID, an edge
    // UUID equal to a node UUID) before it names a missing endpoint. Keep that
    // precedence so the same input is refused the same way on either route.
    // On the deferred route the endpoint leaves answer in their own pass, so
    // the only miss this pass can know is the resident index's.
    let miss = miss
        .into_inner()
        .map_err(|_| storage("endpoint lock poisoned"))?;
    if let Some(endpoint) = miss {
        let node_uuids = match endpoints {
            Endpoints::Resident { nodes, .. } => Some(nodes.uuids.as_slice()),
            Endpoints::Deferred => None,
        };
        return Err(scattered.refusal(scratch, plan, node_uuids, false, Some(endpoint), cancel));
    }
    Ok(scattered)
}

// ------------------------------------------------- deferred reference pass

/// What the deferred route's second edge pass learned.
#[derive(Debug)]
pub(super) struct ReplayedEdges {
    /// The canonical topology proof of the replayed rows; the caller compares
    /// it with the first pass's before any endpoint resolves or anything is
    /// published.
    pub(super) proof: [u8; 32],
    /// The smallest endpoint no node leaf could hold.
    pub(super) miss: Option<[u8; 16]>,
}

/// The typed refusal when the reference pass replayed a different canonical
/// topology than the raw pass accepted: the two versions cannot be mixed, so
/// nothing may resolve or publish over the difference. The aggregate proofs
/// are 256-bit SHA-256 fingerprints with the same collision limits as every
/// other digest, not a byte comparison or a source-property replay claim.
pub(super) fn replay_refusal(
    accepted: Option<[u8; 32]>,
    replayed: [u8; 32],
) -> Result<(), GfError> {
    if accepted == Some(replayed) {
        return Ok(());
    }
    Err(GfError::Api {
        code: graphforge_core::ApiErrorCode::IdentityConflict,
        message: "graph construction encoding: an edge source replayed different rows than \
                  the accepted first pass read; refusing to mix topology versions"
            .to_owned(),
    })
}

/// The second edge pass of the deferred route: replay every planned task of
/// the same fixed task plan, send each row's two endpoint references and its
/// identity probe to the node leaves, and hash the same canonical tuple
/// stream the first pass hashed. Only refs and probes are written: the
/// property rows, the relation dictionary and the raw edge records stay the
/// first pass's accepted output. Validation, admission, the decode gate and
/// cancellation run exactly as in the first pass, and every task's exact row
/// count must match the footer plan before its proof enters the accumulator.
pub(super) fn replay_edges(
    sources: &[BulkSource<'_>],
    budgets: GraphConstructionBudgets,
    decode: &super::gate::ByteGate,
    sink: &RefSink<'_>,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<ReplayedEdges, GfError> {
    let tasks = Tasks::plan(sources, "edges")?;
    let staging = plan.staging_for(sink.refs.len() + sink.probes.len());
    let proof = TopologyProof::new();
    let miss = Mutex::new(None::<[u8; 16]>);
    claim_in_order(tasks.items.clone(), |(source, task, rows)| {
        check_cancelled(cancel)?;
        let _decoding = decode.hold(sources[source].task_decode_bytes(task), cancel)?;
        let mut refs = Scatter::new(scratch, sink.refs, staging);
        let mut probes = Scatter::new(scratch, sink.probes, staging);
        let mut hasher = TaskTopology::start(source, task, rows);
        let mut written = 0;
        let mut uuids = Vec::new();
        let mut source_uuids = Vec::new();
        let mut target_uuids = Vec::new();
        let mut task_miss = None::<[u8; 16]>;
        sources[source].reader.read_task(task, &mut |batch| {
            check_cancelled(cancel)?;
            if !sources[source].reader.admitted() {
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Edge,
                    &batch,
                )?;
                admit_batch(ConstructionChunkKind::Edge, &batch, budgets)?;
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
                crate::graph_construction::batch_uuid_column(&batch, "edge_uuid")?,
                &mut uuids,
            );
            let relations = required_string(&batch, "rel_type")?;
            for (name, column) in [
                ("source_uuid", &mut source_uuids),
                ("target_uuid", &mut target_uuids),
            ] {
                column.clear();
                column.resize(count, [0_u8; 16]);
                copy_uuids(
                    crate::graph_construction::batch_uuid_column(&batch, name)?,
                    column,
                );
            }
            for row in 0..count {
                hasher.row(
                    &uuids[row],
                    &source_uuids[row],
                    &target_uuids[row],
                    relations.value(row),
                );
                for (role, key) in [(ROLE_SRC, source_uuids[row]), (ROLE_DST, target_uuids[row])] {
                    let routed = sink.push(
                        &mut refs,
                        &RefRecord {
                            key,
                            edge: uuids[row],
                            role,
                        },
                    )?;
                    if !routed {
                        task_miss.get_or_insert(key);
                    }
                }
                // The probe only asks whether this edge's own identity is a
                // node's; it carries the UUID alone.
                sink.push_probe(&mut probes, &uuids[row])?;
            }
            written += count;
            Ok(())
        })?;
        if written != rows {
            return Err(short_source());
        }
        refs.finish()?;
        probes.finish()?;
        proof.add_task(hasher.finish())?;
        if let Some(endpoint) = task_miss {
            miss.lock()
                .map_err(|_| storage("endpoint lock poisoned"))?
                .get_or_insert(endpoint);
        }
        Ok(())
    })?;
    Ok(ReplayedEdges {
        proof: proof.finish()?,
        miss: miss
            .into_inner()
            .map_err(|_| storage("endpoint lock poisoned"))?,
    })
}

impl ScatteredEdges {
    /// Partition `part`'s records in UUID order, with identities checked.
    /// `node_uuids` are the sorted node UUIDs, when they are resident.
    fn load_sorted(
        &self,
        scratch: &Scratch,
        part: usize,
        node_uuids: Option<&[[u8; 16]]>,
    ) -> Result<Vec<EdgeRecord>, GfError> {
        let mut records = Vec::with_capacity(usize::try_from(self.counts[part]).map_err(storage)?);
        self.partitions.read(scratch, part, |payload| {
            records.extend(payload.chunks_exact(EDGE_RECORD).map(EdgeRecord::decode));
            Ok(())
        })?;
        if records.len() as u64 != self.counts[part] {
            return Err(storage("an edge scratch partition lost records"));
        }
        records.sort_unstable_by_key(|record| record.uuid);
        if records.windows(2).any(|pair| pair[0].uuid == pair[1].uuid) {
            return Err(storage(
                "duplicate identity across construction runs (edge)",
            ));
        }
        if let (Some(node_uuids), Some(first)) = (node_uuids, records.first()) {
            let mut position = node_uuids.partition_point(|node| node < &first.uuid);
            for record in &records {
                while position < node_uuids.len() && node_uuids[position] < record.uuid {
                    position += 1;
                }
                if position == node_uuids.len() {
                    break;
                }
                if node_uuids[position] == record.uuid {
                    return Err(storage(
                        "duplicate identity across construction runs (edge UUID equals a node UUID)",
                    ));
                }
            }
        }
        // The partition's records are sorted in memory now; the ranking input
        // is loaded and this file is not read again.
        self.partitions.reclaim(scratch, part)?;
        Ok(records)
    }

    /// The refusal for an input whose endpoints did not all resolve, or whose
    /// edge UUIDs collide with node UUIDs, in the order the in-memory build
    /// reports them: a repeated edge UUID, an edge UUID that is a node UUID,
    /// then a missing endpoint. Reads the partitions again; this is the error
    /// path, so the success path never pays for it.
    pub(super) fn refusal(
        &self,
        scratch: &Scratch,
        plan: &ScratchPlan,
        node_uuids: Option<&[[u8; 16]]>,
        collision: bool,
        endpoint: Option<[u8; 16]>,
        cancel: &AtomicBool,
    ) -> GfError {
        let ordered = Ordered::new(plan.gate_bytes, 0, cancel);
        let mut is_edge = false;
        for part in 0..self.partitions.len() {
            if let Err(error) = check_cancelled(cancel) {
                return error;
            }
            let cost = plan.edge_cost(self.counts[part]);
            if let Err(error) = ordered.acquire(part, cost) {
                return error;
            }
            let records = match self.load_sorted(scratch, part, node_uuids) {
                Ok(records) => records,
                Err(error) => return error,
            };
            if let Some(endpoint) = endpoint {
                is_edge |= records
                    .binary_search_by(|record| record.uuid.cmp(&endpoint))
                    .is_ok();
            }
            drop(records);
            ordered.release(cost);
        }
        if collision {
            return storage(
                "duplicate identity across construction runs (edge UUID equals a node UUID)",
            );
        }
        missing_endpoint_error(is_edge)
    }
}

// ------------------------------------------------------------------ pass 3

/// What pass 3 learned about the relation types, in partition order.
pub(super) struct RankedEdges {
    /// Relation ids in the order they first appear in UUID order.
    pub(super) first_appearance: Vec<u32>,
    /// Edges per relation id.
    pub(super) counts: Vec<u64>,
}

pub(super) struct RankContext<'a> {
    pub(super) scratch: &'a Scratch,
    pub(super) scattered: &'a ScatteredEdges,
    pub(super) csr: &'a CsrScratch,
    /// The resident node UUIDs, or the resolved endpoints of scratch node tables.
    pub(super) nodes: RankedNodes<'a>,
    pub(super) emitter: &'a EdgeEmitter<'a>,
    pub(super) plan: &'a ScratchPlan,
    pub(super) window: usize,
    pub(super) cancel: &'a AtomicBool,
}

/// Where pass 3 finds each edge's endpoint ranks and UUIDs.
#[derive(Clone, Copy)]
pub(super) enum RankedNodes<'a> {
    /// Ranks are in the records; UUIDs come from the resident node table.
    Resident(&'a NodeTable),
    /// Ranks and UUIDs come from the resolved endpoints, one file per partition.
    Resolved(&'a Partitions),
}

/// Records of a leaf with the UUIDs of their endpoints (scratch node tables).
#[derive(Default)]
struct Carry {
    records: Vec<EdgeRecord>,
    endpoints: Vec<EndpointPair>,
}

fn rows<'a, T>(
    prefix: &'a [T],
    own: &'a [T],
    range: std::ops::Range<usize>,
) -> impl Iterator<Item = &'a T> {
    // A column that is absent (endpoint UUIDs of a resident-node build) is
    // empty, and every range over it is empty too.
    let carried = range.start.min(prefix.len())..range.end.min(prefix.len());
    let local = range.start.saturating_sub(prefix.len()).min(own.len())
        ..range.end.saturating_sub(prefix.len()).min(own.len());
    prefix[carried].iter().chain(own[local].iter())
}

fn emit_rows<'a>(
    emitter: &EdgeEmitter<'_>,
    first_id: u64,
    selected: impl Iterator<Item = EdgeRecord>,
    endpoints: impl Iterator<Item = &'a EndpointPair>,
) -> Result<(), GfError> {
    let (mut uuids, mut src, mut dst, mut rels) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for record in selected {
        uuids.push(record.uuid);
        src.push(record.src);
        dst.push(record.dst);
        rels.push(record.rel);
    }
    let (mut src_uuids, mut dst_uuids) = (Vec::new(), Vec::new());
    for pair in endpoints {
        src_uuids.push(pair.src);
        dst_uuids.push(pair.dst);
    }
    emitter.emit_window(&EdgeWindow {
        first_id,
        uuids: &uuids,
        src: &src,
        dst: &dst,
        rels: &rels,
        endpoint_uuids: (!src_uuids.is_empty()).then_some((&src_uuids, &dst_uuids)),
    })
}

/// Rank, emit and stage every partition; returns the facts the catalog and
/// the membership index need.
#[allow(clippy::too_many_lines)]
pub(super) fn rank_partitions(context: &RankContext<'_>) -> Result<RankedEdges, GfError> {
    let RankContext {
        scratch,
        scattered,
        csr,
        nodes,
        emitter,
        plan,
        window,
        cancel,
    } = *context;
    let partitions = scattered.partitions.len();
    let mut bases = Vec::with_capacity(partitions);
    let mut running = 0_u64;
    for count in &scattered.counts {
        bases.push(running);
        running += count;
    }
    let relation_count = scattered.rel_names.len();
    // One pair of counters per relation, independent of the leaf count.
    let relation_counts = (0..relation_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>();
    let first_edges = (0..relation_count)
        .map(|_| AtomicU64::new(u64::MAX))
        .collect::<Vec<_>>();
    let carry = Mutex::new(Carry::default());
    let ordered_scatter = csr.out_keys.has_heavy() || csr.in_keys.has_heavy();
    let ordered = Ordered::new(plan.gate_bytes, 2, cancel);
    run_ordered(
        partitions,
        plan.concurrency,
        &ordered,
        |part| plan.edge_cost(scattered.counts[part]),
        |part| {
            let base = bases[part];
            let (records, endpoints) = match nodes {
                RankedNodes::Resident(table) => (
                    scattered.load_sorted(scratch, part, Some(&table.uuids))?,
                    Vec::new(),
                ),
                RankedNodes::Resolved(resolved) => {
                    let mut records = scattered.load_sorted(scratch, part, None)?;
                    let endpoints = join_endpoints(scratch, resolved, part, &mut records)?;
                    (records, endpoints)
                }
            };
            let mut run_relation = None::<u32>;
            let mut run_count = 0_u64;
            let staging = plan.staging_bytes.saturating_mul(2 * plan.csr_partitions)
                / (csr.out.len() + csr.inn.len()).max(1);
            let mut out = Scatter::new(scratch, &csr.out, staging);
            let mut inn = Scatter::new(scratch, &csr.inn, staging);
            // Per-node occurrence ordinals must see globally ranked edges in
            // order. Sorting and canonical emission still overlap; builds
            // without heavy nodes retain parallel CSR scatter.
            if ordered_scatter {
                ordered.wait_turn(1, part)?;
            }
            for (position, record) in records.iter().enumerate() {
                if position % 8192 == 0 {
                    check_cancelled(cancel)?;
                }
                let edge = u32::try_from(base + position as u64 + 1).map_err(storage)?;
                if run_relation != Some(record.rel) {
                    if let Some(relation) = run_relation {
                        relation_counts[relation as usize].fetch_add(run_count, Ordering::Relaxed);
                    }
                    run_relation = Some(record.rel);
                    run_count = 0;
                    let first = &first_edges[record.rel as usize];
                    if u64::from(edge) < first.load(Ordering::Relaxed) {
                        first.fetch_min(u64::from(edge), Ordering::Relaxed);
                    }
                }
                run_count += 1;
                out.push(
                    csr.out_keys.partition(record.src),
                    &CsrRecord {
                        key: record.src,
                        edge,
                        neighbor: record.dst,
                        rel: record.rel,
                    }
                    .encode(),
                )?;
                inn.push(
                    csr.in_keys.partition(record.dst),
                    &CsrRecord {
                        key: record.dst,
                        edge,
                        neighbor: record.src,
                        rel: record.rel,
                    }
                    .encode(),
                )?;
            }
            if let Some(relation) = run_relation {
                relation_counts[relation as usize].fetch_add(run_count, Ordering::Relaxed);
            }
            out.finish()?;
            inn.finish()?;
            if ordered_scatter {
                ordered.pass_turn(1);
            }
            // Windows are fixed ranges of edge ids. This partition completes
            // the windows that end inside it; the rest carries to the next.
            #[cfg(test)]
            std::thread::sleep(std::time::Duration::from_millis(
                plan.stagger_millis * (partitions - part) as u64,
            ));
            ordered.wait_turn(0, part)?;
            let (prefix, prefix_endpoints) = {
                let mut held = carry.lock().map_err(|_| storage("carry lock poisoned"))?;
                let prefix = std::mem::take(&mut held.records);
                let prefix_endpoints = std::mem::take(&mut held.endpoints);
                let total = prefix.len() + records.len();
                let whole = total / window * window;
                held.records
                    .extend(rows(&prefix, &records, whole..total).copied());
                held.endpoints
                    .extend(rows(&prefix_endpoints, &endpoints, whole..total).copied());
                (prefix, prefix_endpoints)
            };
            ordered.pass_turn(0);
            let total = prefix.len() + records.len();
            let first_id = base + 1 - prefix.len() as u64;
            for start in (0..total / window * window).step_by(window) {
                check_cancelled(cancel)?;
                emit_rows(
                    emitter,
                    first_id + start as u64,
                    rows(&prefix, &records, start..start + window).copied(),
                    rows(&prefix_endpoints, &endpoints, start..start + window),
                )?;
            }
            Ok(())
        },
    )?;
    // The rows after the last whole window are the final, short window.
    let tail = carry
        .into_inner()
        .map_err(|_| storage("carry lock poisoned"))?;
    if !tail.records.is_empty() {
        emit_rows(
            emitter,
            scattered.total - tail.records.len() as u64 + 1,
            tail.records.iter().copied(),
            tail.endpoints.iter(),
        )?;
    }
    let counts = relation_counts
        .iter()
        .map(|count| count.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let mut first_appearance = counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count != 0)
        .map(|(relation, _)| u32::try_from(relation).expect("bounded relation dictionary"))
        .collect::<Vec<_>>();
    first_appearance
        .sort_unstable_by_key(|relation| first_edges[*relation as usize].load(Ordering::Relaxed));
    check_cancelled(cancel)?;
    Ok(RankedEdges {
        first_appearance,
        counts,
    })
}

#[cfg(test)]
#[path = "scratch_edges_deferred_tests.rs"]
mod deferred_tests;
