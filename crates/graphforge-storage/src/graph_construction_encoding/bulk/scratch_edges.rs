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
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::StringArray;
use rayon::prelude::*;

use super::budget::ScratchPlan;
use super::emit::{EdgeEmitter, EdgeWindow};
use super::ordered::{Ordered, run_ordered};
use super::scratch::{Appender, Partitions, Scatter, Scratch};
use super::scratch_csr::{CsrRecord, CsrScratch, KeyHistogram};
use super::tables::{
    NodeIndex, NodeTable, Tasks, admit_batch, check_cancelled, copy_uuids, short_source,
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

// ------------------------------------------------------------------ pass 2

/// The edges after pass 2: scattered, unranked.
pub(super) struct ScatteredEdges {
    pub(super) partitions: Partitions,
    splitters: Vec<[u8; 16]>,
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
    nodes: &NodeTable,
    index: &NodeIndex<'_>,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<ScatteredEdges, GfError> {
    let tasks = Tasks::plan(sources, "edges")?;
    let splitters = edge_splitters(sources, &tasks, plan.edge_partitions, cancel)?;
    let partitions = Partitions::create(scratch, "edges", splitters.len() + 1, EDGE_RECORD)?;
    let dictionary = SharedDictionary::default();
    let histogram = KeyHistogram::new(nodes.uuids.len() as u64);
    let miss = Mutex::new(None::<[u8; 16]>);
    tasks
        .items
        .par_iter()
        .try_for_each(|&(source, task, rows)| {
            check_cancelled(cancel)?;
            let mut scatter = Scatter::new(scratch, &partitions, plan.staging_bytes);
            let mut cache = RelationCache {
                shared: &dictionary,
                ids: HashMap::new(),
            };
            let mut local = histogram.local();
            let mut written = 0;
            let mut uuids = Vec::new();
            let mut sources_ranks = Vec::new();
            let mut targets = Vec::new();
            let mut rels = Vec::new();
            let mut endpoints = Vec::new();
            let mut task_miss = None::<[u8; 16]>;
            sources[source].reader.read_task(task, &mut |batch| {
                check_cancelled(cancel)?;
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Edge,
                    &batch,
                )?;
                admit_batch(ConstructionChunkKind::Edge, &batch, budgets)?;
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
                        histogram.add(&mut local, record.src, record.dst);
                    }
                    scatter.push(partition_of(&splitters, &record.uuid), &record.encode())?;
                }
                written += count;
                Ok(())
            })?;
            if written != rows {
                return Err(short_source());
            }
            scatter.finish()?;
            histogram.merge(&local);
            if let Some(endpoint) = task_miss {
                miss.lock()
                    .map_err(|_| storage("endpoint lock poisoned"))?
                    .get_or_insert(endpoint);
            }
            Ok(())
        })?;
    let counts = partitions.counts()?;
    let total = counts.iter().sum::<u64>();
    if total != tasks.total as u64 {
        return Err(short_source());
    }
    let scattered = ScatteredEdges {
        partitions,
        splitters,
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
        for part in 0..scattered.partitions.len() {
            check_cancelled(cancel)?;
            scattered.load_sorted(scratch, part, &nodes.uuids)?;
        }
        let owner = partition_of(&scattered.splitters, &endpoint);
        let records = scattered.load_sorted(scratch, owner, &nodes.uuids)?;
        let is_edge = records
            .binary_search_by(|record| record.uuid.cmp(&endpoint))
            .is_ok();
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
    /// The sorted edge UUIDs, one scratch file per partition, in order.
    pub(super) uuid_files: Vec<PathBuf>,
}

struct PartitionStats {
    first: Vec<u32>,
    counts: Vec<u64>,
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
    let uuid_files = (0..partitions)
        .map(|part| scratch.file(&format!("edge-uuids-{part:06}.blocks")))
        .collect::<Vec<_>>();
    let stats = (0..partitions)
        .map(|_| Mutex::new(None::<PartitionStats>))
        .collect::<Vec<_>>();
    let carry = Mutex::new(Vec::<EdgeRecord>::new());
    let ordered = Ordered::new(plan.gate_bytes, 1, cancel);
    run_ordered(
        partitions,
        plan.concurrency,
        &ordered,
        |part| ScratchPlan::edge_cost(scattered.counts[part]),
        |part| {
            let base = bases[part];
            let records = scattered.load_sorted(scratch, part, &nodes.uuids)?;
            let mut counts = vec![0_u64; relation_count];
            let mut first = Vec::new();
            let mut uuids = Appender::create(scratch, &uuid_files[part], 1 << 20)?;
            let mut out = Scatter::new(scratch, &csr.out, plan.staging_bytes);
            let mut inn = Scatter::new(scratch, &csr.inn, plan.staging_bytes);
            for (position, record) in records.iter().enumerate() {
                if position % 8192 == 0 {
                    check_cancelled(cancel)?;
                }
                let edge = u32::try_from(base + position as u64 + 1).map_err(storage)?;
                if counts[record.rel as usize] == 0 {
                    first.push(record.rel);
                }
                counts[record.rel as usize] += 1;
                uuids.push(&record.uuid)?;
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
            uuids.finish()?;
            out.finish()?;
            inn.finish()?;
            *stats[part]
                .lock()
                .map_err(|_| storage("stats lock poisoned"))? =
                Some(PartitionStats { first, counts });

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
    let mut first_appearance = Vec::new();
    let mut counts = vec![0_u64; relation_count];
    let mut seen = vec![false; relation_count];
    for slot in stats {
        let part = slot
            .into_inner()
            .map_err(|_| storage("stats lock poisoned"))?
            .ok_or_else(|| storage("a scratch partition produced no statistics"))?;
        for relation in part.first {
            if !seen[relation as usize] {
                seen[relation as usize] = true;
                first_appearance.push(relation);
            }
        }
        for (total, count) in counts.iter_mut().zip(part.counts) {
            *total += count;
        }
    }
    check_cancelled(cancel)?;
    let _ = Ordering::Relaxed;
    Ok(RankedEdges {
        first_appearance,
        counts,
        uuid_files,
    })
}
