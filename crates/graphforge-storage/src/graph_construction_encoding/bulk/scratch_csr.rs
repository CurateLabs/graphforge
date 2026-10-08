//! Adjacency CSR of the over-budget bulk build (#1900).
//!
//! Every edge contributes one entry per direction, `(key, edge_id, neighbor,
//! relation)`, scattered once into node-range partitions while the edge
//! partitions are built. A direction's partitions are then sorted one at a
//! time (several in flight, within the memory gate) and cut into shards.
//!
//! The published shard boundaries are a greedy walk over the whole sorted
//! entry sequence of a relation group: a shard closes when it holds
//! `max_edges` entries or when the next entry's key is `max_nodes` or more past
//! the shard's first key. A partition boundary is not a shard boundary, so each
//! relation group keeps the open shard between partitions. That one step runs
//! in partition order (a turn per group); sorting, filtering, and encoding the
//! shards overlap freely. The cuts are the ones the in-memory build makes, so
//! the shards are byte-identical.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use super::budget::ScratchPlan;
use super::csr::{AdjacencyGroups, AdjacencyOutput, assemble, reset_adjacency_directory};
use super::ordered::{Ordered, run_ordered};
use super::scratch::{Partitions, Scratch};
use super::tables::check_cancelled;
use super::{GfError, Path, storage};
use crate::adjacency::{
    AdjacencyBuildOptions, CsrShardRecord, Direction, ShardSetWriter, SortedCsrOutcome,
};

pub(super) const CSR_RECORD: usize = 16;

/// One adjacency entry. Sorting by `(key, edge)` orders every node's list by edge id.
#[derive(Clone, Copy)]
pub(super) struct CsrRecord {
    pub(super) key: u32,
    pub(super) edge: u32,
    pub(super) neighbor: u32,
    pub(super) rel: u32,
}

impl CsrRecord {
    pub(super) fn encode(self) -> [u8; CSR_RECORD] {
        let mut bytes = [0_u8; CSR_RECORD];
        bytes[..4].copy_from_slice(&self.key.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.edge.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.neighbor.to_le_bytes());
        bytes[12..].copy_from_slice(&self.rel.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let word = |index: usize| {
            u32::from_le_bytes(bytes[index * 4..index * 4 + 4].try_into().expect("4 bytes"))
        };
        Self {
            key: word(0),
            edge: word(1),
            neighbor: word(2),
            rel: word(3),
        }
    }

    fn order(&self) -> u64 {
        (u64::from(self.key) << 32) | u64::from(self.edge)
    }
}

// ------------------------------------------------------------- histogram

/// Exact degree counts choose bounded consecutive `(node, edge)` ranges.
pub(super) struct KeyHistogram {
    out: Vec<AtomicU32>,
    inn: Vec<AtomicU32>,
    edges: u64,
    max_entries: u64,
}

impl KeyHistogram {
    pub(super) fn new(nodes: u64, edges: u64, max_entries: u64) -> Self {
        let zeroed = || (0..nodes).map(|_| AtomicU32::new(0)).collect();
        Self {
            out: zeroed(),
            inn: zeroed(),
            edges,
            max_entries: max_entries.max(1),
        }
    }

    pub(super) fn add(&self, src: u32, dst: u32) {
        self.out[src as usize - 1].fetch_add(1, Ordering::Relaxed);
        self.inn[dst as usize - 1].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn partitioner(&self, direction: Direction, wanted: usize) -> KeyPartitioner {
        let degrees = match direction {
            Direction::Out => &self.out,
            Direction::In => &self.inn,
        };
        let limit = self
            .max_entries
            .min(self.edges.div_ceil(wanted.max(1) as u64).max(1));
        let mut table = Vec::with_capacity(degrees.len());
        let heavy_count = degrees
            .iter()
            .filter(|degree| u64::from(degree.load(Ordering::Relaxed)) > limit)
            .count();
        let mut seen = Vec::with_capacity(heavy_count);
        let (mut part, mut used) = (0_u32, 0_u64);
        for degree in degrees {
            let degree = u64::from(degree.load(Ordering::Relaxed));
            if degree > limit {
                if used != 0 {
                    part += 1;
                    used = 0;
                }
                let counter = u32::try_from(seen.len() + 1).expect("dense node ids");
                table.push((part, counter));
                seen.push(AtomicU32::new(0));
                // Ranking visits edge partitions in order when there is a
                // heavy node, so its own occurrence ordinal orders its edges.
                part += u32::try_from(degree.div_ceil(limit)).expect("dense edge ids");
            } else {
                if used + degree > limit {
                    part += 1;
                    used = 0;
                }
                table.push((part, 0));
                used += degree;
            }
        }
        let count = (part as usize + 1).max(wanted);
        KeyPartitioner {
            table,
            span: limit,
            count,
            seen,
        }
    }
}

/// A light node shares one partition with consecutive light nodes. A heavy
/// node occupies consecutive partitions split by increasing occurrence ordinal.
pub(super) struct KeyPartitioner {
    table: Vec<(u32, u32)>,
    span: u64,
    count: usize,
    seen: Vec<AtomicU32>,
}

impl KeyPartitioner {
    pub(super) fn has_heavy(&self) -> bool {
        !self.seen.is_empty()
    }

    /// Heavy nodes require calls in increasing edge-id order.
    pub(super) fn partition(&self, key: u32) -> usize {
        let (base, counter) = self.table[key as usize - 1];
        base as usize
            + if counter == 0 {
                0
            } else {
                let ordinal = self.seen[counter as usize - 1].fetch_add(1, Ordering::Relaxed);
                usize::try_from(u64::from(ordinal) / self.span).expect("dense edge ids")
            }
    }
}

// -------------------------------------------------------------- partitions

/// The node-range partitions of both directions and how they split the keys.
pub(super) struct CsrScratch {
    pub(super) out: Partitions,
    pub(super) inn: Partitions,
    pub(super) out_keys: KeyPartitioner,
    pub(super) in_keys: KeyPartitioner,
}

impl CsrScratch {
    pub(super) fn create(
        scratch: &Scratch,
        histogram: &KeyHistogram,
        partitions: usize,
    ) -> Result<Self, GfError> {
        let out_keys = histogram.partitioner(Direction::Out, partitions);
        let in_keys = histogram.partitioner(Direction::In, partitions);
        Ok(Self {
            out: Partitions::create(scratch, "csr-out", out_keys.count, CSR_RECORD)?,
            inn: Partitions::create(scratch, "csr-in", in_keys.count, CSR_RECORD)?,
            out_keys,
            in_keys,
        })
    }
}

// -------------------------------------------------------------- shard cuts

/// The open shard of one relation group between partitions.
#[derive(Default)]
struct GroupState {
    carry: Vec<CsrRecord>,
    ordinal: usize,
    entries: u64,
    last_key: Option<u32>,
}

/// A closed shard: the carried entries, then `view[range]`.
struct Job {
    ordinal: usize,
    prefix: Vec<CsrRecord>,
    range: Range<usize>,
}

/// Advance `state` over the sorted `view` and return the shards it closes.
fn cut(state: &mut GroupState, view: &[CsrRecord], max_edges: usize, max_nodes: u64) -> Vec<Job> {
    let mut jobs = Vec::new();
    if let Some(last) = view.last() {
        state.last_key = Some(last.key);
    }
    state.entries += view.len() as u64;
    let mut position = 0;
    while position < view.len() {
        let (first, room) = match state.carry.first() {
            None => (u64::from(view[position].key), max_edges),
            Some(head) => (u64::from(head.key), max_edges - state.carry.len()),
        };
        let by_nodes = position
            + view[position..].partition_point(|record| u64::from(record.key) - first < max_nodes);
        let end = by_nodes.min(position + room);
        if end == view.len() {
            // Whether the shard is full is decided by the next entry, which
            // may be in the next partition.
            state.carry.extend_from_slice(&view[position..end]);
            break;
        }
        jobs.push(Job {
            ordinal: state.ordinal,
            prefix: std::mem::take(&mut state.carry),
            range: position..end,
        });
        state.ordinal += 1;
        position = end;
    }
    jobs
}

fn encode_job(
    set: &ShardSetWriter,
    job: &Job,
    view: &[CsrRecord],
) -> Result<(usize, CsrShardRecord), GfError> {
    let body = &view[job.range.clone()];
    let first = job
        .prefix
        .first()
        .or(body.first())
        .expect("a shard has entries");
    let last = body
        .last()
        .or(job.prefix.last())
        .expect("a shard has entries");
    let record = set.write_shard(
        job.ordinal,
        u64::from(first.key),
        u64::from(last.key),
        job.prefix.iter().chain(body).map(|entry| {
            (
                u64::from(entry.key),
                u64::from(entry.edge),
                u64::from(entry.neighbor),
            )
        }),
    )?;
    Ok((job.ordinal, record))
}

/// What the adjacency pass needs besides the partitions.
pub(super) struct AdjacencyContext<'a> {
    pub(super) scratch: &'a Scratch,
    pub(super) plan: &'a ScratchPlan,
    pub(super) graph_root: &'a Path,
    pub(super) groups: &'a AdjacencyGroups,
    pub(super) generation: u64,
    pub(super) built_at_micros: i64,
    pub(super) total_edges: u64,
    pub(super) allocation: Option<&'a crate::StorageAllocationOperation>,
    pub(super) options: &'a AdjacencyBuildOptions,
    pub(super) cancel: &'a AtomicBool,
}

fn load_sorted(
    scratch: &Scratch,
    partitions: &Partitions,
    index: usize,
    expected: u64,
) -> Result<Vec<CsrRecord>, GfError> {
    let mut records = Vec::with_capacity(usize::try_from(expected).map_err(storage)?);
    partitions.read(scratch, index, |payload| {
        records.extend(payload.chunks_exact(CSR_RECORD).map(CsrRecord::decode));
        Ok(())
    })?;
    if records.len() as u64 != expected {
        return Err(storage("a CSR scratch partition lost entries"));
    }
    records.sort_unstable_by_key(CsrRecord::order);
    Ok(records)
}

#[allow(clippy::too_many_lines)]
fn write_direction(
    context: &AdjacencyContext<'_>,
    partitions: &Partitions,
    direction: Direction,
    outcomes: &mut BTreeMap<(usize, bool), SortedCsrOutcome>,
) -> Result<(), GfError> {
    let groups = context.groups;
    let sets = groups
        .stems
        .iter()
        .map(|stem| {
            ShardSetWriter::create(
                &crate::adjacency::csr_path(context.graph_root, stem, direction),
                context.options.shard_max_edges,
                context.options.shard_max_nodes,
                context.allocation,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (max_edges, max_nodes) = (sets[0].max_edges(), sets[0].max_nodes());
    let counts = partitions.counts()?;
    let states = (0..sets.len())
        .map(|_| Mutex::new(GroupState::default()))
        .collect::<Vec<_>>();
    let records = (0..sets.len())
        .map(|_| Mutex::new(Vec::<(usize, CsrShardRecord)>::new()))
        .collect::<Vec<_>>();
    let ordered = Ordered::new(context.plan.gate_bytes, sets.len(), context.cancel);
    run_ordered(
        partitions.len(),
        context.plan.concurrency,
        &ordered,
        |index| ScratchPlan::csr_cost(counts[index]),
        |index| {
            let sorted = load_sorted(context.scratch, partitions, index, counts[index])?;
            for group in 0..sets.len() {
                check_cancelled(context.cancel)?;
                let filtered;
                let view: &[CsrRecord] = if groups.is_whole(group) {
                    &sorted
                } else {
                    let wanted = u32::try_from(group).map_err(storage)?;
                    filtered = sorted
                        .iter()
                        .filter(|record| groups.relation_group[record.rel as usize] == wanted)
                        .copied()
                        .collect::<Vec<_>>();
                    &filtered
                };
                ordered.wait_turn(group, index)?;
                let jobs = {
                    let mut state = states[group]
                        .lock()
                        .map_err(|_| storage("CSR group lock poisoned"))?;
                    cut(&mut state, view, max_edges, max_nodes)
                };
                ordered.pass_turn(group);
                for job in &jobs {
                    let written = encode_job(&sets[group], job, view)?;
                    records[group]
                        .lock()
                        .map_err(|_| storage("CSR record lock poisoned"))?
                        .push(written);
                }
            }
            Ok(())
        },
    )?;
    let no_entries: &[CsrRecord] = &[];
    for (group, set) in sets.into_iter().enumerate() {
        check_cancelled(context.cancel)?;
        let state = states[group]
            .lock()
            .map_err(|_| storage("CSR group lock poisoned"))?;
        let mut written = records[group]
            .lock()
            .map_err(|_| storage("CSR record lock poisoned"))?;
        if !state.carry.is_empty() {
            let last = Job {
                ordinal: state.ordinal,
                prefix: state.carry.clone(),
                range: 0..0,
            };
            written.push(encode_job(&set, &last, no_entries)?);
        }
        written.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        let shard_records = written.drain(..).map(|(_, record)| record).collect();
        let node_count = state.last_key.map_or(0, |key| u64::from(key) + 1);
        outcomes.insert(
            (group, matches!(direction, Direction::In)),
            set.finish(shard_records, state.entries, node_count)?,
        );
    }
    Ok(())
}

/// Build every relation group's CSR shards in both directions from the
/// scattered entries, and publish the adjacency manifest.
pub(super) fn write_scratch_adjacency(
    context: &AdjacencyContext<'_>,
    csr: &CsrScratch,
) -> Result<AdjacencyOutput, GfError> {
    reset_adjacency_directory(context.graph_root)?;
    let mut outcomes = BTreeMap::new();
    write_direction(context, &csr.out, Direction::Out, &mut outcomes)?;
    write_direction(context, &csr.inn, Direction::In, &mut outcomes)?;
    assemble(
        context.graph_root,
        context.groups,
        outcomes,
        context.generation,
        context.built_at_micros,
        context.total_edges,
        context.allocation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: u32, edge: u32) -> CsrRecord {
        CsrRecord {
            key,
            edge,
            neighbor: 0,
            rel: 0,
        }
    }

    /// The cuts of one pass over `entries`, however they are split into partitions.
    fn shards(
        entries: &[CsrRecord],
        split: &[usize],
        max_edges: usize,
        max_nodes: u64,
    ) -> Vec<Vec<u32>> {
        let mut state = GroupState::default();
        let mut closed = Vec::new();
        let mut from = 0;
        for to in split.iter().copied().chain([entries.len()]) {
            let view = &entries[from..to];
            for job in cut(&mut state, view, max_edges, max_nodes) {
                closed.push(
                    job.prefix
                        .iter()
                        .chain(&view[job.range])
                        .map(|entry| entry.edge)
                        .collect(),
                );
            }
            from = to;
        }
        if !state.carry.is_empty() {
            closed.push(state.carry.iter().map(|entry| entry.edge).collect());
        }
        closed
    }

    #[test]
    fn shard_cuts_do_not_depend_on_where_the_partitions_split() {
        // Keys with a hub (key 3 has 7 entries), gaps, and a long tail.
        let mut entries = Vec::new();
        let mut edge = 0;
        for (key, degree) in [
            (1, 2),
            (2, 1),
            (3, 7),
            (6, 3),
            (7, 1),
            (40, 4),
            (41, 5),
            (90, 2),
        ] {
            for _ in 0..degree {
                edge += 1;
                entries.push(record(key, edge));
            }
        }
        for (max_edges, max_nodes) in [(1, 1), (3, 100), (4, 5), (5, 3), (100, 2), (100, 100)] {
            let whole = shards(&entries, &[], max_edges, max_nodes);
            // Walk the specification: the greedy rule over the whole sequence.
            let mut expected = Vec::<Vec<u32>>::new();
            let mut first = 0_u64;
            for entry in &entries {
                let full = expected.last().is_none_or(|shard| {
                    shard.len() >= max_edges || u64::from(entry.key) - first >= max_nodes
                });
                if full {
                    expected.push(Vec::new());
                    first = u64::from(entry.key);
                }
                expected.last_mut().unwrap().push(entry.edge);
            }
            assert_eq!(
                whole, expected,
                "max_edges={max_edges} max_nodes={max_nodes}"
            );
            for split in [
                vec![1],
                vec![5],
                vec![3, 3, 9],
                vec![2, 4, 6, 8, 10, 12, 20],
                vec![0, 25],
            ] {
                assert_eq!(
                    shards(&entries, &split, max_edges, max_nodes),
                    expected,
                    "split {split:?} max_edges={max_edges} max_nodes={max_nodes}"
                );
            }
        }
    }

    #[test]
    fn many_heavy_nodes_need_only_their_own_degree_partitions() {
        let histogram = KeyHistogram::new(100, 10_100, 100);
        for key in 1..=100 {
            for _ in 0..101 {
                histogram.add(key, key);
            }
        }
        let partitioner = histogram.partitioner(Direction::Out, 1);
        assert_eq!(partitioner.count, 201);
        let mut counts = vec![0; partitioner.count];
        // Edge ids can be interleaved across nodes; each node's occurrence
        // ordinal still creates ordered, bounded ranges in the final CSR.
        for _ in 0..101 {
            for key in 1..=100 {
                counts[partitioner.partition(key)] += 1;
            }
        }
        assert!(counts.iter().all(|count| *count <= 100));
        for key in 0..100 {
            assert_eq!(&counts[2 * key..2 * key + 2], &[100, 1]);
            assert_eq!(partitioner.table[key].0 as usize, 2 * key);
        }
    }

    #[test]
    fn key_partitions_are_monotone_and_balanced_by_entries() {
        let histogram = KeyHistogram::new(1000, 1400, 500);
        // A hot node near the front and a flat tail.
        for _ in 0..400 {
            histogram.add(5, 900);
        }
        for node in 1..=1000_u32 {
            histogram.add(node, node);
        }
        let partitioner = histogram.partitioner(Direction::Out, 4);
        let mut previous = 0;
        let mut sizes = vec![0_u32; partitioner.count];
        for key in 1..=1000_u32 {
            for _ in 0..if key == 5 { 401 } else { 1 } {
                let part = partitioner.partition(key);
                assert!(
                    part >= previous,
                    "partition ids never decrease with the key"
                );
                previous = part;
                sizes[part] += 1;
            }
        }
        assert_eq!(sizes.iter().sum::<u32>(), 1400);
        assert!(
            sizes.iter().all(|size| *size > 0 && *size <= 350),
            "{sizes:?}"
        );
    }
}
