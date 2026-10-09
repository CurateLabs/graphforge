//! Adjacency CSR of the over-budget bulk build (#1900).
//!
//! Every edge contributes one entry per direction, `(key, edge_id, neighbor,
//! relation)`, scattered once into node-range partitions while the edge
//! partitions are built. A direction's partitions are then sorted one at a
//! time within the memory gate. The union is encoded directly; relation
//! entries are spooled in sorted order, then encoded one relation at a time.
//!
//! The published shard boundaries are a greedy walk over the whole sorted
//! entry sequence of a relation group: a shard closes when it holds
//! `max_edges` entries or when the next entry's key is `max_nodes` or more past
//! the shard's first key. A partition boundary is not a shard boundary, so each
//! stream keeps its open shard between partitions or spool blocks. Only one
//! carry and one shard encoder exist at once, independent of relation count.
//! Relation spooling adds one scratch write/read for each non-covering usable
//! relation entry; covering groups share the union's carry. The cuts are the
//! ones the in-memory build makes, so the shards are byte-identical.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use super::budget::ScratchPlan;
use super::csr::{AdjacencyGroups, AdjacencyOutput, assemble, reset_adjacency_directory};
use super::ordered::Ordered;
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
    pub(super) peak_carry_entries: AtomicU64,
    pub(super) spool_write_bytes: AtomicU64,
    pub(super) spool_read_bytes: AtomicU64,
}

impl CsrScratch {
    pub(super) fn peak_carry_entries(&self) -> u64 {
        self.peak_carry_entries.load(Ordering::Relaxed)
    }

    pub(super) fn csr_spool_write_bytes(&self) -> u64 {
        self.spool_write_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn csr_spool_read_bytes(&self) -> u64 {
        self.spool_read_bytes.load(Ordering::Relaxed)
    }

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
            peak_carry_entries: AtomicU64::new(0),
            spool_write_bytes: AtomicU64::new(0),
            spool_read_bytes: AtomicU64::new(0),
        })
    }
}

// -------------------------------------------------------------- shard cuts

/// One reusable canonical shard: relation streams run consecutively, so neither
/// the carry nor its encoder multiplies with the number of relation groups.
struct ShardCarry {
    records: Vec<CsrRecord>,
    max_edges: usize,
    max_nodes: u64,
    entries: u64,
    last_key: Option<u32>,
    peak_entries: usize,
}

impl ShardCarry {
    fn new(max_edges: usize, max_nodes: u64) -> Self {
        Self {
            // Exact capacity prevents geometric growth from exceeding the
            // separately reserved one-shard workspace.
            records: Vec::with_capacity(max_edges),
            max_edges,
            max_nodes,
            entries: 0,
            last_key: None,
            peak_entries: 0,
        }
    }

    fn push(
        &mut self,
        entry: CsrRecord,
        emit: &mut impl FnMut(&[CsrRecord]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.records.len() == self.max_edges
            || self
                .records
                .first()
                .is_some_and(|first| u64::from(entry.key) - u64::from(first.key) >= self.max_nodes)
        {
            self.flush(emit)?;
        }
        self.records.push(entry);
        self.peak_entries = self.peak_entries.max(self.records.len());
        self.entries += 1;
        self.last_key = Some(entry.key);
        Ok(())
    }

    fn flush(
        &mut self,
        emit: &mut impl FnMut(&[CsrRecord]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if !self.records.is_empty() {
            emit(&self.records)?;
            self.records.clear();
        }
        Ok(())
    }

    fn node_count(&self) -> u64 {
        self.last_key.map_or(0, |key| u64::from(key) + 1)
    }

    fn reset(&mut self) {
        debug_assert!(self.records.is_empty());
        self.entries = 0;
        self.last_key = None;
    }
}

fn encode_shard(
    set: &ShardSetWriter,
    ordinal: usize,
    entries: &[CsrRecord],
) -> Result<CsrShardRecord, GfError> {
    set.write_shard(
        ordinal,
        u64::from(entries.first().expect("a shard has entries").key),
        u64::from(entries.last().expect("a shard has entries").key),
        entries.iter().map(|entry| {
            (
                u64::from(entry.key),
                u64::from(entry.edge),
                u64::from(entry.neighbor),
            )
        }),
    )
}

/// Append sorted relation segments through one bounded staging block. Files
/// open only for an append; no relation owns a persistent buffer or fd.
fn spool_relations(
    context: &AdjacencyContext<'_>,
    spools: &Partitions,
    sorted: &mut [CsrRecord],
    block: &mut Vec<u8>,
) -> Result<(), GfError> {
    let group_of = |record: &CsrRecord| context.groups.relation_group[record.rel as usize];
    sorted.sort_unstable_by_key(|record| (group_of(record), record.order()));
    let capacity = block.capacity();
    let mut from = 0;
    while from < sorted.len() {
        let group = group_of(&sorted[from]);
        let to = from + sorted[from..].partition_point(|record| group_of(record) == group);
        if group != u32::MAX && !context.groups.is_whole(group as usize) {
            for segment in sorted[from..to].chunks((capacity - 8) / CSR_RECORD) {
                check_cancelled(context.cancel)?;
                block.clear();
                block.extend_from_slice(&[0; 8]);
                for entry in segment {
                    block.extend_from_slice(&entry.encode());
                }
                spools.append(context.scratch, group as usize, block)?;
            }
        }
        from = to;
    }
    Ok(())
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
    csr: &CsrScratch,
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
    let spools = Partitions::create(
        context.scratch,
        &format!("csr-{}-relations", direction.as_str()),
        sets.len(),
        CSR_RECORD,
    )?;
    let counts = partitions.counts()?;
    let mut records = (0..sets.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut carry = ShardCarry::new(max_edges, max_nodes);
    let block_bytes = context.plan.staging_bytes.clamp(CSR_RECORD, 64 << 10);
    let mut block = Vec::with_capacity(8 + block_bytes / CSR_RECORD * CSR_RECORD);
    let ordered = Ordered::new(context.plan.gate_bytes, 1, context.cancel);
    let whole_groups = (0..sets.len())
        .filter(|group| groups.is_whole(*group))
        .collect::<Vec<_>>();
    let mut emit_whole = |entries: &[CsrRecord]| {
        // Covering groups and the union share the SAME carry. Encode their
        // identical shard sequentially, keeping only one encoder alive.
        for &group in &whole_groups {
            check_cancelled(context.cancel)?;
            let record = encode_shard(&sets[group], records[group].len(), entries)?;
            records[group].push(record);
        }
        Ok(())
    };
    for (index, count) in counts.into_iter().enumerate() {
        check_cancelled(context.cancel)?;
        let cost = ScratchPlan::csr_cost(count);
        ordered.acquire(index, cost)?;
        let outcome = (|| {
            let mut sorted = load_sorted(context.scratch, partitions, index, count)?;
            for chunk in sorted.chunks(4096) {
                check_cancelled(context.cancel)?;
                for entry in chunk {
                    carry.push(*entry, &mut emit_whole)?;
                }
            }
            let before = context.scratch.written_bytes();
            spool_relations(context, &spools, &mut sorted, &mut block)?;
            csr.spool_write_bytes
                .fetch_add(context.scratch.written_bytes() - before, Ordering::Relaxed);
            Ok::<_, GfError>(())
        })();
        ordered.release(cost);
        outcome?;
    }
    carry.flush(&mut emit_whole)?;
    let (whole_edges, whole_nodes) = (carry.entries, carry.node_count());
    let mut totals = vec![(whole_edges, whole_nodes); sets.len()];
    let spool_read_before = context.scratch.read_bytes();
    let spool_counts = spools.counts()?;
    // Only relation metadata remains resident. Stream each ordered spool
    // through the reused carry, checking every scratch block's CRC32C.
    for (group, set) in sets
        .iter()
        .enumerate()
        .filter(|(group, _)| !groups.is_whole(*group))
    {
        check_cancelled(context.cancel)?;
        carry.reset();
        let mut emit = |entries: &[CsrRecord]| {
            check_cancelled(context.cancel)?;
            let record = encode_shard(set, records[group].len(), entries)?;
            records[group].push(record);
            Ok(())
        };
        spools.read(context.scratch, group, |payload| {
            check_cancelled(context.cancel)?;
            for entry in payload.chunks_exact(CSR_RECORD).map(CsrRecord::decode) {
                carry.push(entry, &mut emit)?;
            }
            Ok(())
        })?;
        if carry.entries != spool_counts[group] {
            return Err(storage("a CSR relation spool lost entries"));
        }
        carry.flush(&mut emit)?;
        totals[group] = (carry.entries, carry.node_count());
    }
    csr.spool_read_bytes.fetch_add(
        context.scratch.read_bytes() - spool_read_before,
        Ordering::Relaxed,
    );
    csr.peak_carry_entries
        .fetch_max(carry.peak_entries as u64, Ordering::Relaxed);
    for (group, (set, shard_records)) in sets.into_iter().zip(records).enumerate() {
        check_cancelled(context.cancel)?;
        let (edges, nodes) = totals[group];
        outcomes.insert(
            (group, matches!(direction, Direction::In)),
            set.finish(shard_records, edges, nodes)?,
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
    write_direction(context, &csr.out, Direction::Out, &mut outcomes, csr)?;
    write_direction(context, &csr.inn, Direction::In, &mut outcomes, csr)?;
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
        let mut state = ShardCarry::new(max_edges, max_nodes);
        let mut closed = Vec::new();
        let mut emit = |entries: &[CsrRecord]| {
            closed.push(entries.iter().map(|entry| entry.edge).collect());
            Ok(())
        };
        let mut from = 0;
        for to in split.iter().copied().chain([entries.len()]) {
            for entry in &entries[from..to] {
                state.push(*entry, &mut emit).unwrap();
            }
            from = to;
        }
        state.flush(&mut emit).unwrap();
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

    /// Exercises the real spool reader and canonical encoder, including groups
    /// whose unfinished shards would otherwise remain resident simultaneously.
    #[allow(clippy::too_many_lines)]
    fn assert_spool_artifact_parity(group_count: usize, max_nodes: usize) {
        use super::super::StableDirectory;
        use super::super::emit::RelationRoute;
        use super::super::scratch::Scatter;

        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let relations = (0..group_count)
            .map(|group| RelationRoute {
                logical: format!("relation_{group:03}"),
                topology_route: format!("relation_{group:03}"),
                qualified: false,
                exploratory: false,
            })
            .collect::<Vec<_>>();
        let groups = AdjacencyGroups::new(&relations).unwrap();
        let mut entries = Vec::new();
        for key in [1, 2, 3, 8, 9, 17, 18, 40, 41] {
            for rel in 0..group_count {
                for _ in 0..4 {
                    let edge = u32::try_from(entries.len() + 1).unwrap();
                    entries.push(CsrRecord {
                        key,
                        edge,
                        neighbor: edge % 17 + 1,
                        rel: u32::try_from(rel).unwrap(),
                    });
                }
            }
        }
        let partitions =
            Partitions::create(&scratch, "input", entries.len().div_ceil(21), CSR_RECORD).unwrap();
        let mut scatter = Scatter::new(&scratch, &partitions, 64);
        for (index, partition) in entries.chunks(21).enumerate() {
            // Arrival order within a partition has no effect on the output.
            for entry in partition.iter().rev() {
                scatter.push(index, &entry.encode()).unwrap();
            }
        }
        scatter.finish().unwrap();
        let histogram = KeyHistogram::new(41, entries.len() as u64, 21);
        let csr = CsrScratch::create(&scratch, &histogram, 1).unwrap();
        let plan = ScratchPlan {
            concurrency: 1,
            edge_partitions: 1,
            csr_partitions: partitions.len(),
            gate_bytes: ScratchPlan::csr_cost(21),
            staging_bytes: 64,
            property: super::super::property_rows::PropertySizing::SERIAL,
        };
        let cancel = AtomicBool::new(false);
        let options = AdjacencyBuildOptions {
            shard_max_edges: 64,
            shard_max_nodes: max_nodes,
            ..AdjacencyBuildOptions::default()
        };
        let actual_root = root.path().join("actual");
        let context = AdjacencyContext {
            scratch: &scratch,
            plan: &plan,
            graph_root: &actual_root,
            groups: &groups,
            generation: 1,
            built_at_micros: 0,
            total_edges: entries.len() as u64,
            allocation: None,
            options: &options,
            cancel: &cancel,
        };
        let mut outcomes = BTreeMap::new();
        write_direction(&context, &partitions, Direction::Out, &mut outcomes, &csr).unwrap();
        let neighbors = entries
            .iter()
            .map(|entry| entry.neighbor)
            .collect::<Vec<_>>();
        for group in 0..groups.stems.len() {
            let sorted = entries
                .iter()
                .filter(|entry| {
                    groups.is_whole(group)
                        || groups.relation_group[entry.rel as usize] as usize == group
                })
                .map(CsrRecord::order)
                .collect::<Vec<_>>();
            let expected = crate::adjacency::write_sharded_csr_from_sorted(
                &crate::adjacency::csr_path(
                    &root.path().join("expected"),
                    &groups.stems[group],
                    Direction::Out,
                ),
                &sorted,
                &neighbors,
                options.shard_max_edges,
                options.shard_max_nodes,
                None,
            )
            .unwrap();
            let actual = &outcomes[&(group, false)];
            assert_eq!(
                (actual.edge_count, actual.node_count, actual.shards),
                (expected.edge_count, expected.node_count, expected.shards)
            );
            assert_eq!(actual.captured.len(), expected.captured.len());
            for (actual, expected) in actual.captured.iter().zip(&expected.captured) {
                assert_eq!(
                    std::fs::read(&actual.path).unwrap(),
                    std::fs::read(&expected.path).unwrap(),
                    "group {group}"
                );
            }
        }
        assert_eq!(csr.csr_spool_read_bytes(), csr.csr_spool_write_bytes());
        assert!(csr.peak_carry_entries() <= options.shard_max_edges as u64);
        if group_count == 1 {
            assert_eq!(
                csr.csr_spool_write_bytes(),
                0,
                "a covering group shares the union carry"
            );
        } else {
            assert!(csr.csr_spool_write_bytes() >= (entries.len() * CSR_RECORD) as u64);
            assert!(
                entries.len() > options.shard_max_edges * 10,
                "aggregate tails exceed one shard"
            );
        }
    }

    #[test]
    fn many_relation_spools_preserve_canonical_bytes_with_one_carry() {
        assert_spool_artifact_parity(32, 64);
        assert_spool_artifact_parity(32, 5);
    }

    #[test]
    fn covering_relation_reuses_union_carry_without_spooling() {
        assert_spool_artifact_parity(1, 64);
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
