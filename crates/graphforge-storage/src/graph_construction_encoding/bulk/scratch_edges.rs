//! Passes 2 and 3 of the over-budget bulk build (#1900).
//!
//! Pass 2 decodes the edges once, resolves their endpoints through the node
//! index, and scatters a compact record (UUID, source and target rank,
//! relation id) into edge-UUID range partitions on scratch. The range
//! boundaries come from a sample of the edge UUIDs.
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
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::StringArray;
use rayon::prelude::*;

use super::budget::ScratchPlan;
use super::emit::{EdgeEmitter, EdgeWindow};
use super::ordered::{Ordered, run_ordered};
use super::scratch::{Appender, Partitions, Scatter, Scratch, read_blocks};
use super::scratch_csr::{CsrRecord, CsrScratch, KeyHistogram};
use super::tables::{
    NodeIndex, NodeTable, Tasks, admit_batch, check_cancelled, claim_in_order, copy_uuids,
    short_source,
};
use super::{
    BulkSource, ConstructionChunkKind, GfError, GraphConstructionBudgets, required_string, storage,
};

pub(super) const EDGE_RECORD: usize = 28;

/// A resolved edge before it has a rank.
#[derive(Clone, Copy)]
pub(super) struct EdgeRecord {
    pub(super) uuid: [u8; 16],
    pub(super) src: u32,
    pub(super) dst: u32,
    pub(super) rel: u32,
}

impl EdgeRecord {
    fn encode(&self) -> [u8; EDGE_RECORD] {
        let mut bytes = [0_u8; EDGE_RECORD];
        bytes[..16].copy_from_slice(&self.uuid);
        bytes[16..20].copy_from_slice(&self.src.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.dst.to_le_bytes());
        bytes[24..].copy_from_slice(&self.rel.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
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

/// Relation names shared by every task, so a record carries its final id.
/// Ids are in first-seen order, which varies between runs; no output depends
/// on it, because every consumer orders by name or by the sorted edges.
#[derive(Default)]
struct SharedDictionary {
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

    fn into_names(self) -> Result<Vec<String>, GfError> {
        self.inner
            .into_inner()
            .map(|inner| inner.0)
            .map_err(|_| storage("relation dictionary lock poisoned"))
    }
}

/// One task's view of the shared dictionary: names it has already resolved.
struct RelationCache<'a> {
    shared: &'a SharedDictionary,
    ids: HashMap<String, u32>,
}

impl RelationCache<'_> {
    fn column(&mut self, values: &StringArray, out: &mut [u32]) -> Result<(), GfError> {
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

// ------------------------------------------------------------- splitters

/// Histogram resolution of the footer-bound splitters, as a power of two.
const BOUND_BUCKET_BITS: u32 = 20;

/// Boundaries from the tasks' footer bounds, when every task states them: each
/// task's rows are spread evenly between its bounds, which is exact for UUIDs
/// that arrive in order and uniform for UUIDs that do not. The histogram spans
/// the smallest to the largest bound, not the whole UUID space: time-ordered
/// identities share their leading bytes.
#[allow(clippy::cast_precision_loss)] // row counts are estimates here
fn splitters_from_bounds(
    sources: &[BulkSource<'_>],
    tasks: &Tasks,
    wanted: usize,
) -> Option<Vec<[u8; 16]>> {
    let mut stated = Vec::with_capacity(tasks.items.len());
    for &(source, task, rows) in &tasks.items {
        if rows > 0 {
            let (low, high) = sources[source].reader.uuid_bounds(task)?;
            let (low, high) = (u128::from_be_bytes(low), u128::from_be_bytes(high));
            if high < low {
                return None;
            }
            stated.push((rows as f64, low, high));
        }
    }
    let origin = stated.iter().map(|(_, low, _)| *low).min()?;
    let end = stated.iter().map(|(_, _, high)| *high).max()?;
    // Buckets of `1 << shift` UUIDs, at most `1 << BOUND_BUCKET_BITS` of them.
    let shift = (128 - (end - origin).leading_zeros()).saturating_sub(BOUND_BUCKET_BITS);
    let buckets = usize::try_from((end - origin) >> shift).ok()? + 1;
    let bucket = |value: u128| usize::try_from((value - origin) >> shift).ok();
    let mut slope = vec![0.0_f64; buckets + 1];
    let mut total = 0.0_f64;
    for (rows, low, high) in stated {
        let (first, last) = (bucket(low)?, bucket(high)?);
        let rate = rows / (last - first + 1) as f64;
        slope[first] += rate;
        slope[last + 1] -= rate;
        total += rows;
    }
    let mut splitters = Vec::new();
    let (mut rate, mut cumulative, mut next) = (0.0_f64, 0.0_f64, 1_usize);
    for (index, change) in slope.iter().take(buckets).enumerate() {
        rate += change;
        cumulative += rate;
        while next < wanted && cumulative >= total * next as f64 / wanted as f64 {
            // Everything up to this bucket sorts below the boundary.
            if index + 1 < buckets {
                splitters.push((origin + (((index + 1) as u128) << shift)).to_be_bytes());
            }
            next += 1;
        }
    }
    splitters.dedup();
    Some(splitters)
}

/// Edge-UUID range boundaries that split the edges into about `wanted`
/// partitions of equal size.
///
/// They come from the row-group bounds in the footers when the source states
/// them. Otherwise (Arrow files, or UUIDs with nulls, which are derived) from a
/// sample of evenly spread tasks: the minimum and maximum of a group of
/// unordered UUIDs say nothing about how they are distributed, but a sample
/// of tasks does, and it covers sorted input too.
fn edge_splitters(
    sources: &[BulkSource<'_>],
    tasks: &Tasks,
    wanted: usize,
    decode: &super::gate::ByteGate,
    cancel: &AtomicBool,
) -> Result<Vec<[u8; 16]>, GfError> {
    if wanted <= 1 || tasks.items.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(splitters) = splitters_from_bounds(sources, tasks, wanted) {
        return Ok(splitters);
    }
    let chosen_count = tasks.items.len().min(64);
    let chosen = (0..chosen_count)
        .map(|index| tasks.items[index * tasks.items.len() / chosen_count])
        .collect::<Vec<_>>();
    let chosen_rows = chosen.iter().map(|(_, _, rows)| *rows).sum::<usize>();
    // Enough samples per partition for a balanced split, never the whole input.
    let step = (chosen_rows / (wanted * 256).max(16_384)).max(1);
    let mut sample = chosen
        .par_iter()
        .map(|&(source, task, _)| {
            check_cancelled(cancel)?;
            let _decoding = decode.hold_task(sources[source].task_decode_bytes(task), cancel)?;
            let mut uuids = Vec::new();
            let mut seen = 0_usize;
            sources[source].reader.read_task(task, &mut |batch| {
                let column = crate::graph_construction::batch_uuid_column(&batch, "edge_uuid")?;
                for row in 0..batch.num_rows() {
                    if seen.is_multiple_of(step) {
                        uuids.push(
                            <[u8; 16]>::try_from(column.value(row)).expect("16-byte identity"),
                        );
                    }
                    seen += 1;
                }
                Ok(())
            })?;
            Ok(uuids)
        })
        .collect::<Result<Vec<_>, GfError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if sample.is_empty() {
        return Ok(Vec::new());
    }
    sample.par_sort_unstable();
    let mut splitters = (1..wanted)
        .map(|part| sample[part * sample.len() / wanted])
        .collect::<Vec<_>>();
    splitters.dedup();
    Ok(splitters)
}

fn partition_of(splitters: &[[u8; 16]], uuid: &[u8; 16]) -> usize {
    splitters.partition_point(|splitter| splitter <= uuid)
}

// -------------------------------------------------------- skew refinement

type UuidBounds = Option<([u8; 16], [u8; 16])>;

fn observe(bounds: &mut UuidBounds, uuid: [u8; 16]) {
    *bounds = Some(bounds.map_or((uuid, uuid), |(low, high)| (low.min(uuid), high.max(uuid))));
}

/// Only oversized UUID ranges take extra scratch passes. A radix step splits
/// at the first differing byte of the observed bounds, skipping common UUID
/// prefixes without rereading them. Equal UUIDs always follow the same leaf.
struct Refinement<'a> {
    scratch: &'a Scratch,
    limit: u64,
    staging_bytes: usize,
    cancel: &'a AtomicBool,
    steps: u64,
    leaves: Vec<(PathBuf, u64)>,
    pending: Option<PendingLeaf<'a>>,
    outputs: u64,
}

struct PendingLeaf<'a> {
    writer: Appender<'a>,
    path: PathBuf,
    rows: u64,
}

impl Refinement<'_> {
    fn finish_pending(&mut self) -> Result<(), GfError> {
        if let Some(PendingLeaf { writer, path, rows }) = self.pending.take() {
            writer.finish()?;
            self.leaves.push((path, rows));
        }
        Ok(())
    }

    /// Greedily pack adjacent radix leaves without ever retaining their path
    /// inventory. One open output consumes each tiny leaf once; it never
    /// recopies the accumulated output when a subsequent leaf arrives.
    fn leaf(&mut self, path: PathBuf, rows: u64, coalesce: bool) -> Result<(), GfError> {
        if !coalesce {
            self.finish_pending()?;
            self.leaves.push((path, rows));
            return Ok(());
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.rows + rows > self.limit)
        {
            self.finish_pending()?;
        }
        if rows == self.limit && self.pending.is_none() {
            self.leaves.push((path, rows));
            return Ok(());
        }
        if self.pending.is_none() {
            let output = self
                .scratch
                .file(&format!("edge-coalesced-{:06}.blocks", self.outputs));
            self.outputs += 1;
            self.pending = Some(PendingLeaf {
                writer: Appender::create(self.scratch, &output, self.staging_bytes)?,
                path: output,
                rows: 0,
            });
        }
        let pending = self.pending.as_mut().expect("an output is open");
        let mut copied = 0_u64;
        read_blocks(self.scratch, &path, |payload| {
            check_cancelled(self.cancel)?;
            if !payload.len().is_multiple_of(EDGE_RECORD) {
                return Err(storage("an edge scratch block has a partial record"));
            }
            for record in payload.chunks_exact(EDGE_RECORD) {
                pending.writer.push(record)?;
                copied += 1;
            }
            Ok(())
        })?;
        if copied != rows {
            return Err(storage("an edge scratch partition lost records"));
        }
        pending.rows += copied;
        std::fs::remove_file(path).map_err(storage)?;
        if pending.rows == self.limit {
            self.finish_pending()?;
        }
        Ok(())
    }

    fn partition(
        &mut self,
        path: PathBuf,
        rows: u64,
        bounds: UuidBounds,
        coalesce: bool,
    ) -> Result<(), GfError> {
        check_cancelled(self.cancel)?;
        if rows <= self.limit {
            return self.leaf(path, rows, coalesce);
        }
        let (low, high) =
            bounds.ok_or_else(|| storage("an edge partition lost its UUID bounds"))?;
        let Some(byte) = low.iter().zip(high).position(|(low, high)| *low != high) else {
            return Err(storage(
                "duplicate identity across construction runs (edge)",
            ));
        };
        let prefix = format!("edge-refinement-{:06}", self.steps);
        self.steps += 1;
        let children = Partitions::create(self.scratch, &prefix, 256, EDGE_RECORD)?;
        let mut scatter = Scatter::new(self.scratch, &children, self.staging_bytes);
        let mut bounds = vec![None; 256];
        let mut read = 0_u64;
        read_blocks(self.scratch, &path, |payload| {
            check_cancelled(self.cancel)?;
            if !payload.len().is_multiple_of(EDGE_RECORD) {
                return Err(storage("an edge scratch block has a partial record"));
            }
            for bytes in payload.chunks_exact(EDGE_RECORD) {
                let uuid: [u8; 16] = bytes[..16].try_into().expect("16-byte identity");
                let child = usize::from(uuid[byte]);
                observe(&mut bounds[child], uuid);
                scatter.push(child, bytes)?;
                read += 1;
            }
            crate::graph_construction::construction_failpoint("bulk.during_edge_refinement");
            Ok(())
        })?;
        if read != rows {
            return Err(storage("an edge scratch partition lost records"));
        }
        scatter.finish()?;
        let counts = children.counts()?;
        std::fs::remove_file(path).map_err(storage)?;
        for (child, count) in counts.into_iter().enumerate() {
            let path = children.path(child).to_path_buf();
            if count == 0 {
                std::fs::remove_file(path).map_err(storage)?;
            } else {
                self.partition(path, count, bounds[child], true)?;
            }
        }
        Ok(())
    }
}

/// Return a globally UUID-ordered inventory whose nonempty leaves all fit a
/// worker's reservation. Every refinement reads a parent once and writes its
/// children once. Streaming coalescing then packs small adjacent children to
/// keep the final inventory proportional to total rows divided by the limit;
/// it does not reopen the registered input source.
fn refine_partitions(
    partitions: &Partitions,
    bounds: &[UuidBounds],
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<(Partitions, u64, u64, u64), GfError> {
    let written = scratch.written_bytes();
    let read = scratch.read_bytes();
    // Sequential refinement shares one total scatter allowance across all 256
    // children. Each child's capacity is rounded down to a whole record.
    let staging = usize::try_from((plan.gate_bytes / 16 / 256).max(EDGE_RECORD as u64))
        .unwrap_or(EDGE_RECORD)
        .min(8 << 10);
    let mut refinement = Refinement {
        scratch,
        limit: (plan.gate_bytes / (2 * plan.concurrency as u64) / 44).max(1),
        staging_bytes: staging,
        cancel,
        steps: 0,
        leaves: Vec::new(),
        pending: None,
        outputs: 0,
    };
    let counts = partitions.counts()?;
    for (part, count) in counts.into_iter().enumerate() {
        refinement.partition(
            partitions.path(part).to_path_buf(),
            count,
            bounds[part],
            false,
        )?;
    }
    refinement.finish_pending()?;
    Ok((
        Partitions::from_inventory(refinement.leaves, EDGE_RECORD),
        scratch.written_bytes() - written,
        scratch.read_bytes() - read,
        refinement.steps,
    ))
}

// ------------------------------------------------------------------ pass 2

/// The edges after pass 2: scattered, unranked.
pub(super) struct ScatteredEdges {
    pub(super) partitions: Partitions,
    pub(super) refinement_write_bytes: u64,
    pub(super) refinement_read_bytes: u64,
    pub(super) refinement_steps: u64,
    pub(super) counts: Vec<u64>,
    pub(super) rel_names: Vec<String>,
    pub(super) histogram: KeyHistogram,
    pub(super) total: u64,
}

fn missing_endpoint_error(is_edge: bool) -> GfError {
    storage(if is_edge {
        "edge endpoint is not a node UUID"
    } else {
        "edge endpoint UUID does not exist"
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn scatter_edges(
    sources: &[BulkSource<'_>],
    budgets: GraphConstructionBudgets,
    properties: Option<&super::property_rows::PropertyRows<'_>>,
    decode: &super::gate::ByteGate,
    nodes: &NodeTable,
    index: &NodeIndex<'_>,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<ScatteredEdges, GfError> {
    let tasks = Tasks::plan(sources, "edges")?;
    let splitters = edge_splitters(sources, &tasks, plan.edge_partitions, decode, cancel)?;
    let partitions = Partitions::create(scratch, "edges", splitters.len() + 1, EDGE_RECORD)?;
    let bounds = (0..partitions.len())
        .map(|_| Mutex::new(None::<([u8; 16], [u8; 16])>))
        .collect::<Vec<_>>();
    let dictionary = SharedDictionary::default();
    let histogram = KeyHistogram::new(
        nodes.uuids.len() as u64,
        tasks.total as u64,
        plan.gate_bytes / (2 * plan.concurrency as u64) / 40,
    );
    let miss = Mutex::new(None::<[u8; 16]>);
    claim_in_order(tasks.items.clone(), |(source, task, rows)| {
        check_cancelled(cancel)?;
        // The bytes this task decodes are reserved before it reads.
        let _decoding = decode.hold_task(sources[source].task_decode_bytes(task), cancel)?;
        let mut scatter = Scatter::new(scratch, &partitions, plan.staging_bytes);
        let mut cache = RelationCache {
            shared: &dictionary,
            ids: HashMap::new(),
        };
        let mut written = 0;
        let mut uuids = Vec::new();
        let mut sources_ranks = Vec::new();
        let mut targets = Vec::new();
        let mut rels = Vec::new();
        let mut endpoints = Vec::new();
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
            cache.column(required_string(&batch, "rel_type")?, &mut rels)?;
            for (name, ranks) in [
                ("source_uuid", &mut sources_ranks),
                ("target_uuid", &mut targets),
            ] {
                endpoints.clear();
                endpoints.resize(count, [0_u8; 16]);
                copy_uuids(
                    crate::graph_construction::batch_uuid_column(&batch, name)?,
                    &mut endpoints,
                );
                ranks.clear();
                ranks.extend(endpoints.iter().map(|endpoint| {
                    index.find(endpoint).unwrap_or_else(|| {
                        task_miss.get_or_insert(*endpoint);
                        0
                    })
                }));
            }
            for row in 0..count {
                let record = EdgeRecord {
                    uuid: uuids[row],
                    src: sources_ranks[row],
                    dst: targets[row],
                    rel: rels[row],
                };
                if record.src != 0 && record.dst != 0 {
                    histogram.add(record.src, record.dst);
                }
                let part = partition_of(&splitters, &record.uuid);
                observe(&mut task_bounds[part], record.uuid);
                scatter.push(part, &record.encode())?;
            }
            if let Some(sink) = &mut sink {
                sink.push(&batch, cancel)?;
            }
            written += count;
            Ok(())
        })?;
        if written != rows {
            return Err(short_source());
        }
        if let Some(sink) = sink {
            sink.finish(cancel)?;
        }
        scatter.finish()?;
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
        .collect::<Result<Vec<_>, _>>()?;
    let (partitions, refinement_write_bytes, refinement_read_bytes, refinement_steps) =
        refine_partitions(&partitions, &bounds, plan, scratch, cancel)?;
    let counts = partitions.counts()?;
    let scattered = ScatteredEdges {
        partitions,
        refinement_write_bytes,
        refinement_read_bytes,
        refinement_steps,
        counts,
        rel_names: dictionary.into_names()?,
        histogram,
        total,
    };
    // The in-memory build checks identities (a repeated edge UUID, an edge
    // UUID equal to a node UUID) before it names a missing endpoint. Keep that
    // precedence so the same input is refused the same way on either route.
    let miss = miss
        .into_inner()
        .map_err(|_| storage("endpoint lock poisoned"))?;
    if let Some(endpoint) = miss {
        let ordered = Ordered::new(plan.gate_bytes, 0, cancel);
        let mut is_edge = false;
        for part in 0..scattered.partitions.len() {
            check_cancelled(cancel)?;
            let cost = ScratchPlan::edge_cost(scattered.counts[part]);
            ordered.acquire(part, cost)?;
            let records = scattered.load_sorted(scratch, part, &nodes.uuids)?;
            is_edge |= records
                .binary_search_by(|record| record.uuid.cmp(&endpoint))
                .is_ok();
            drop(records);
            ordered.release(cost);
        }
        return Err(missing_endpoint_error(is_edge));
    }
    Ok(scattered)
}

impl ScatteredEdges {
    /// Partition `part`'s records in UUID order, with identities checked.
    fn load_sorted(
        &self,
        scratch: &Scratch,
        part: usize,
        node_uuids: &[[u8; 16]],
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
        if let Some(first) = records.first() {
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
        Ok(records)
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
    pub(super) nodes: &'a NodeTable,
    pub(super) emitter: &'a EdgeEmitter<'a>,
    pub(super) plan: &'a ScratchPlan,
    pub(super) window: usize,
    pub(super) cancel: &'a AtomicBool,
}

fn rows<'a>(
    prefix: &'a [EdgeRecord],
    own: &'a [EdgeRecord],
    range: std::ops::Range<usize>,
) -> impl Iterator<Item = &'a EdgeRecord> {
    let carried = range.start.min(prefix.len())..range.end.min(prefix.len());
    let local = range.start.saturating_sub(prefix.len())..range.end.saturating_sub(prefix.len());
    prefix[carried].iter().chain(own[local].iter())
}

fn emit_rows(
    emitter: &EdgeEmitter<'_>,
    first_id: u64,
    selected: impl Iterator<Item = EdgeRecord>,
) -> Result<(), GfError> {
    let (mut uuids, mut src, mut dst, mut rels) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for record in selected {
        uuids.push(record.uuid);
        src.push(record.src);
        dst.push(record.dst);
        rels.push(record.rel);
    }
    emitter.emit_window(&EdgeWindow {
        first_id,
        uuids: &uuids,
        src: &src,
        dst: &dst,
        rels: &rels,
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
    let carry = Mutex::new(Vec::<EdgeRecord>::new());
    let ordered_scatter = csr.out_keys.has_heavy() || csr.in_keys.has_heavy();
    let ordered = Ordered::new(plan.gate_bytes, 2, cancel);
    run_ordered(
        partitions,
        plan.concurrency,
        &ordered,
        |part| ScratchPlan::edge_cost(scattered.counts[part]),
        |part| {
            let base = bases[part];
            let records = scattered.load_sorted(scratch, part, &nodes.uuids)?;
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
            ordered.wait_turn(0, part)?;
            let prefix = {
                let mut held = carry.lock().map_err(|_| storage("carry lock poisoned"))?;
                let prefix = std::mem::take(&mut *held);
                let total = prefix.len() + records.len();
                let whole = total / window * window;
                held.extend(rows(&prefix, &records, whole..total).copied());
                prefix
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
                )?;
            }
            Ok(())
        },
    )?;
    // The rows after the last whole window are the final, short window.
    let tail = carry
        .into_inner()
        .map_err(|_| storage("carry lock poisoned"))?;
    if !tail.is_empty() {
        emit_rows(
            emitter,
            scattered.total - tail.len() as u64 + 1,
            tail.iter().copied(),
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
mod refinement_tests {
    use super::*;
    use crate::graph_construction_encoding::StableDirectory;

    fn initial(scratch: &Scratch, uuids: &[[u8; 16]]) -> (Partitions, Vec<UuidBounds>) {
        let partitions = Partitions::create(scratch, "initial", 1, EDGE_RECORD).unwrap();
        let mut scatter = Scatter::new(scratch, &partitions, 28 * 20);
        let mut bounds = None;
        for (index, uuid) in uuids.iter().copied().enumerate() {
            observe(&mut bounds, uuid);
            scatter
                .push(
                    0,
                    &EdgeRecord {
                        uuid,
                        src: u32::try_from(index + 1).unwrap(),
                        dst: 3,
                        rel: 7,
                    }
                    .encode(),
                )
                .unwrap();
        }
        scatter.finish().unwrap();
        (partitions, vec![bounds])
    }

    fn plan(limit: u64) -> ScratchPlan {
        ScratchPlan {
            concurrency: 1,
            edge_partitions: 1,
            csr_partitions: 1,
            gate_bytes: limit * 44 * 2,
            staging_bytes: 4096,
            property: super::super::property_rows::PropertySizing::SERIAL,
            decode_bytes: 0,
        }
    }

    #[test]
    fn clustered_uuids_and_outliers_refine_into_bounded_ordered_ranges() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut uuids = (0..600_u16)
            .rev()
            .map(|index| {
                let mut uuid = [0x77; 16];
                uuid[14..].copy_from_slice(&index.to_be_bytes());
                uuid
            })
            .collect::<Vec<_>>();
        uuids.extend([[0; 16], [0xff; 16]]);
        let (partitions, bounds) = initial(&scratch, &uuids);
        let original = partitions.path(0).to_path_buf();
        let cancel = AtomicBool::new(false);
        let (partitions, written, read, steps) =
            refine_partitions(&partitions, &bounds, &plan(40), &scratch, &cancel).unwrap();
        assert!(!original.exists());
        assert!(steps >= 3);
        assert!(written > 0 && read > 0);
        assert!(
            partitions
                .counts()
                .unwrap()
                .iter()
                .all(|count| *count <= 40)
        );
        let mut got = Vec::new();
        for part in 0..partitions.len() {
            let mut records = Vec::new();
            partitions
                .read(&scratch, part, |payload| {
                    records.extend(payload.chunks_exact(EDGE_RECORD).map(EdgeRecord::decode));
                    Ok(())
                })
                .unwrap();
            records.sort_unstable_by_key(|record| record.uuid);
            got.extend(
                records
                    .into_iter()
                    .map(|record| (record.uuid, record.src, record.dst, record.rel)),
            );
        }
        let mut expected = uuids
            .into_iter()
            .enumerate()
            .map(|(index, uuid)| (uuid, u32::try_from(index + 1).unwrap(), 3, 7))
            .collect::<Vec<_>>();
        expected.sort_unstable_by_key(|record| record.0);
        assert_eq!(got, expected);
        assert_eq!(scratch.read_bytes(), scratch.written_bytes());
    }

    #[test]
    fn repeated_radix_fanout_coalesces_small_children_as_they_arrive() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut uuids = Vec::new();
        // Every group is one row over the limit, yet a byte split yields
        // 255 singleton outliers and one two-row child.
        for group in 0..8_u8 {
            for child in 0..=255_u8 {
                let mut uuid = [0x66; 16];
                uuid[1] = group;
                uuid[2] = child;
                uuid[15] = 0;
                uuids.push(uuid);
                if child == 0 {
                    uuid[15] = 1;
                    uuids.push(uuid);
                }
            }
        }
        let expected = uuids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let rows = uuids.len() as u64;
        let (partitions, bounds) = initial(&scratch, &uuids);
        let cancel = AtomicBool::new(false);
        let (partitions, _, _, _) =
            refine_partitions(&partitions, &bounds, &plan(256), &scratch, &cancel).unwrap();
        assert!(partitions.len() as u64 <= 1 + 2 * rows.div_ceil(256));
        assert!(
            partitions
                .counts()
                .unwrap()
                .iter()
                .all(|count| *count <= 256)
        );
        let mut got = Vec::new();
        for part in 0..partitions.len() {
            let mut local = Vec::new();
            partitions
                .read(&scratch, part, |payload| {
                    local.extend(
                        payload
                            .chunks_exact(EDGE_RECORD)
                            .map(|bytes| EdgeRecord::decode(bytes).uuid),
                    );
                    Ok(())
                })
                .unwrap();
            local.sort_unstable();
            got.extend(local);
        }
        assert_eq!(got, expected.into_iter().collect::<Vec<_>>());
        assert_eq!(scratch.read_bytes(), scratch.written_bytes());
    }

    #[test]
    fn a_shared_fifteen_byte_prefix_is_skipped_in_one_refinement() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let uuids = (0..200_u8)
            .map(|last| {
                let mut uuid = [0x44; 16];
                uuid[15] = last;
                uuid
            })
            .collect::<Vec<_>>();
        let (partitions, bounds) = initial(&scratch, &uuids);
        let cancel = AtomicBool::new(false);
        let (_, _, _, steps) =
            refine_partitions(&partitions, &bounds, &plan(10), &scratch, &cancel).unwrap();
        assert_eq!(steps, 1);
    }

    #[test]
    fn refinement_checks_the_parent_block_crc_before_using_records() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let uuids = (0..100_u8)
            .map(|last| {
                let mut uuid = [0x44; 16];
                uuid[15] = last;
                uuid
            })
            .collect::<Vec<_>>();
        let (partitions, bounds) = initial(&scratch, &uuids);
        let path = partitions.path(0);
        let mut bytes = std::fs::read(path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let cancel = AtomicBool::new(false);
        let error = refine_partitions(&partitions, &bounds, &plan(10), &scratch, &cancel)
            .err()
            .unwrap();
        assert!(error.to_string().contains("CRC32C"), "{error}");
    }

    #[test]
    fn an_oversized_equal_uuid_range_is_refused_without_recursion() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let (partitions, bounds) = initial(&scratch, &[[0x44; 16]; 100]);
        let cancel = AtomicBool::new(false);
        let error = refine_partitions(&partitions, &bounds, &plan(10), &scratch, &cancel)
            .err()
            .unwrap();
        assert!(error.to_string().contains("duplicate identity"), "{error}");
    }
}
