//! Passes 1 and 2: decode, validate, order and rank nodes and edges.
//!
//! Identifiers are dense `u32` ranks while the graph has fewer than 2^32 nodes
//! and edges. UUIDs exist only in the sorted arrays and at the output.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use rayon::prelude::*;

use super::{
    BulkSource, ConstructionChunkKind, FixedSizeBinaryArray, GfError, GraphConstructionBudgets,
    RecordBatch, StringArray, required_string, storage,
};

/// Largest node or edge count the dense `u32` ranks can address.
const MAX_DENSE: u64 = u32::MAX as u64 - 1;

fn cancelled_error() -> GfError {
    storage("construction encoding cancelled")
}

pub(super) fn check_cancelled(cancel: &AtomicBool) -> Result<(), GfError> {
    if cancel.load(Ordering::Acquire) {
        return Err(cancelled_error());
    }
    Ok(())
}

pub(super) fn require_dense(count: u64, what: &str) -> Result<(), GfError> {
    if count > MAX_DENSE {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            message: format!("graph construction encoding: {what} exceed the dense u32 rank space"),
        });
    }
    Ok(())
}

#[derive(Default)]
struct LocalDictionary {
    names: Vec<String>,
    ids: HashMap<String, u32>,
}

impl LocalDictionary {
    fn id(&mut self, name: &str) -> u32 {
        if let Some(id) = self.ids.get(name) {
            return *id;
        }
        let id = u32::try_from(self.names.len()).expect("bounded by rows per task");
        self.names.push(name.to_owned());
        self.ids.insert(name.to_owned(), id);
        id
    }

    fn column(&mut self, values: &StringArray, out: &mut [u32]) {
        let mut previous: Option<(&str, u32)> = None;
        for (row, slot) in out.iter_mut().enumerate() {
            let value = values.value(row);
            let id = match previous {
                Some((text, id)) if text == value => id,
                _ => self.id(value),
            };
            previous = Some((value, id));
            *slot = id;
        }
    }
}

fn copy_uuids(array: &FixedSizeBinaryArray, out: &mut [[u8; 16]]) {
    for (slot, bytes) in out.iter_mut().zip(array.value_data().chunks_exact(16)) {
        *slot = <[u8; 16]>::try_from(bytes).expect("16-byte chunk");
    }
}

/// The exact rows each task will emit, from the footers, and where they land in
/// the assembled columns. Every task decodes straight into its own slice of the
/// final arrays, so a decoded copy and an assembled copy never coexist.
struct Tasks {
    /// `(source, task, rows)` in input order.
    items: Vec<(usize, usize, usize)>,
    total: usize,
}

impl Tasks {
    fn plan(sources: &[BulkSource<'_>], what: &str) -> Result<Self, GfError> {
        let mut items = Vec::new();
        let mut total = 0_usize;
        for (source, planned) in sources.iter().enumerate() {
            for task in 0..planned.tasks {
                let rows = planned.reader.task_rows(task);
                total = total
                    .checked_add(rows)
                    .ok_or_else(|| storage("source row count overflows"))?;
                items.push((source, task, rows));
            }
        }
        require_dense(total as u64, what)?;
        Ok(Self { items, total })
    }

    /// One mutable slice of `column` per task.
    fn carve<'a, T>(&self, column: &'a mut [T]) -> Vec<&'a mut [T]> {
        let mut rest = column;
        self.items
            .iter()
            .map(|&(_, _, rows)| {
                let (head, tail) = std::mem::take(&mut rest).split_at_mut(rows);
                rest = tail;
                head
            })
            .collect()
    }
}

/// Merge the tasks' local dictionaries into one and return each task's
/// local-to-global id map.
fn global_dictionary(dictionaries: &[&LocalDictionary]) -> (Vec<String>, Vec<Vec<u32>>) {
    let mut global = LocalDictionary::default();
    let maps = dictionaries
        .iter()
        .map(|dictionary| {
            dictionary
                .names
                .iter()
                .map(|name| global.id(name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    (global.names, maps)
}

fn remap_in_place(tasks: &Tasks, column: &mut [u32], maps: &[Vec<u32>]) {
    tasks
        .carve(column)
        .into_par_iter()
        .zip(maps.par_iter())
        .for_each(|(slice, map)| {
            for id in slice {
                *id = map[*id as usize];
            }
        });
}

/// The staged path's per-chunk admission, applied to every decoded batch: the
/// property-column, row and byte windows. (Its run window is implied: a budget
/// set validates `max_run_records >= 4 * max_batch_rows`.)
fn admit_batch(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    let required = match kind {
        ConstructionChunkKind::Node => 2,
        ConstructionChunkKind::Edge => 4,
    };
    if batch.num_columns().saturating_sub(required) > budgets.max_property_columns {
        return Err(storage("construction property-column budget exhausted"));
    }
    if batch.num_rows() > budgets.max_batch_rows
        || batch.get_array_memory_size() > budgets.max_batch_bytes
    {
        return Err(storage("construction resource window exhausted"));
    }
    Ok(())
}

fn short_source() -> GfError {
    storage("a source emitted a different row count than its footer")
}

fn gather<T: Copy + Send + Sync>(source: &[T], order: &[u32]) -> Vec<T> {
    order
        .par_iter()
        .map(|&index| source[index as usize])
        .collect()
}

/// Make `uuids` strictly increasing. Returns the applied permutation (new
/// position -> old position) when the input was not already sorted.
fn sort_unique(uuids: &mut Vec<[u8; 16]>, what: &str) -> Result<Option<Vec<u32>>, GfError> {
    let sorted = uuids.par_windows(2).all(|pair| pair[0] <= pair[1]);
    let order = if sorted {
        None
    } else {
        let count = u32::try_from(uuids.len()).map_err(storage)?;
        let mut order = (0..count).collect::<Vec<_>>();
        order
            .par_sort_unstable_by(|&left, &right| uuids[left as usize].cmp(&uuids[right as usize]));
        *uuids = gather(uuids, &order);
        Some(order)
    };
    if uuids.par_windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage(format!(
            "duplicate identity across construction runs ({what})"
        )));
    }
    Ok(order)
}

// ---------------------------------------------------------------- nodes

#[derive(Default)]
struct NodeChunk {
    dictionary: LocalDictionary,
    kept: Vec<RecordBatch>,
}

/// Sorted node identities with their label ids; `node_id` is `index + 1`.
pub(super) struct NodeTable {
    pub(super) uuids: Vec<[u8; 16]>,
    pub(super) labels: Vec<u32>,
    pub(super) label_names: Vec<String>,
    /// Canonical batches in input order, kept only for property-bearing input.
    pub(super) kept: Vec<RecordBatch>,
}

pub(super) fn collect_nodes(
    sources: &[BulkSource<'_>],
    retain: bool,
    budgets: GraphConstructionBudgets,
    cancel: &AtomicBool,
) -> Result<NodeTable, GfError> {
    let tasks = Tasks::plan(sources, "nodes")?;
    // Zero-allocated: pages become resident only as tasks write them.
    let mut uuids = vec![[0_u8; 16]; tasks.total];
    let mut labels = vec![0_u32; tasks.total];
    let chunks = tasks
        .items
        .par_iter()
        .zip(tasks.carve(&mut uuids))
        .zip(tasks.carve(&mut labels))
        .map(|((&(source, task, rows), uuids), labels)| {
            check_cancelled(cancel)?;
            let mut chunk = NodeChunk::default();
            let mut written = 0;
            sources[source].reader.read_task(task, &mut |batch| {
                check_cancelled(cancel)?;
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Node,
                    &batch,
                )?;
                admit_batch(ConstructionChunkKind::Node, &batch, budgets)?;
                let count = batch.num_rows();
                if written + count > rows {
                    return Err(short_source());
                }
                if count == 0 {
                    return Ok(());
                }
                copy_uuids(
                    crate::graph_construction::batch_uuid_column(&batch, "node_uuid")?,
                    &mut uuids[written..written + count],
                );
                chunk.dictionary.column(
                    required_string(&batch, "label")?,
                    &mut labels[written..written + count],
                );
                written += count;
                if retain {
                    chunk.kept.push(batch);
                }
                Ok(())
            })?;
            if written != rows {
                return Err(short_source());
            }
            Ok(chunk)
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let dictionaries = chunks
        .iter()
        .map(|chunk| &chunk.dictionary)
        .collect::<Vec<_>>();
    let (label_names, maps) = global_dictionary(&dictionaries);
    remap_in_place(&tasks, &mut labels, &maps);
    let kept = chunks
        .into_iter()
        .flat_map(|chunk| chunk.kept)
        .collect::<Vec<_>>();
    if let Some(order) = sort_unique(&mut uuids, "node")? {
        labels = gather(&labels, &order);
    }
    Ok(NodeTable {
        uuids,
        labels,
        label_names,
        kept,
    })
}

// ----------------------------------------------------------- node index

/// Open-addressing index from node UUID to its dense rank (ADR 0057). The
/// table stores ranks; keys are compared against the sorted UUID array.
pub(super) struct NodeIndex<'a> {
    uuids: &'a [[u8; 16]],
    slots: Vec<AtomicU32>,
    mask: usize,
}

fn mix(uuid: &[u8; 16]) -> usize {
    let low = u64::from_le_bytes(uuid[..8].try_into().expect("8 bytes"));
    let high = u64::from_le_bytes(uuid[8..].try_into().expect("8 bytes"));
    let mut value = low.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ high.rotate_left(29);
    value = (value ^ (value >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    value ^= value >> 32;
    // Truncation on a 32-bit target only drops hash bits.
    #[allow(clippy::cast_possible_truncation)]
    {
        value as usize
    }
}

impl<'a> NodeIndex<'a> {
    pub(super) fn build(uuids: &'a [[u8; 16]]) -> Self {
        let size = (uuids.len().saturating_mul(2)).next_power_of_two().max(16);
        let slots = (0..size).map(|_| AtomicU32::new(0)).collect::<Vec<_>>();
        let mask = size - 1;
        uuids.par_iter().enumerate().for_each(|(index, uuid)| {
            let rank = u32::try_from(index + 1).expect("dense rank");
            let mut position = mix(uuid) & mask;
            while slots[position]
                .compare_exchange(0, rank, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
            {
                position = (position + 1) & mask;
            }
        });
        Self { uuids, slots, mask }
    }

    /// Dense rank (1-based) of `uuid`, if it is a node.
    #[inline]
    pub(super) fn find(&self, uuid: &[u8; 16]) -> Option<u32> {
        let mut position = mix(uuid) & self.mask;
        loop {
            let rank = self.slots[position].load(Ordering::Relaxed);
            if rank == 0 {
                return None;
            }
            if self.uuids[rank as usize - 1] == *uuid {
                return Some(rank);
            }
            position = (position + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- edges

#[derive(Default)]
struct EdgeChunk {
    dictionary: LocalDictionary,
    kept: Vec<RecordBatch>,
    miss: Option<[u8; 16]>,
}

/// Sorted edge identities with resolved endpoint ranks and relation ids;
/// `edge_id` is `index + 1`.
pub(super) struct EdgeTable {
    pub(super) uuids: Vec<[u8; 16]>,
    pub(super) src: Vec<u32>,
    pub(super) dst: Vec<u32>,
    pub(super) rels: Vec<u32>,
    pub(super) rel_names: Vec<String>,
    pub(super) kept: Vec<RecordBatch>,
}

#[allow(clippy::too_many_lines)]
pub(super) fn collect_edges(
    sources: &[BulkSource<'_>],
    retain: bool,
    budgets: GraphConstructionBudgets,
    nodes: &NodeTable,
    index: &NodeIndex<'_>,
    cancel: &AtomicBool,
) -> Result<EdgeTable, GfError> {
    let tasks = Tasks::plan(sources, "edges")?;
    let mut uuids = vec![[0_u8; 16]; tasks.total];
    let mut src = vec![0_u32; tasks.total];
    let mut dst = vec![0_u32; tasks.total];
    let mut rels = vec![0_u32; tasks.total];
    let chunks = tasks
        .items
        .par_iter()
        .zip(tasks.carve(&mut uuids))
        .zip(tasks.carve(&mut src))
        .zip(tasks.carve(&mut dst))
        .zip(tasks.carve(&mut rels))
        .map(|((((&(source, task, rows), uuids), src), dst), rels)| {
            check_cancelled(cancel)?;
            let mut chunk = EdgeChunk::default();
            let mut written = 0;
            let mut endpoints = Vec::new();
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
                let range = written..written + count;
                copy_uuids(
                    crate::graph_construction::batch_uuid_column(&batch, "edge_uuid")?,
                    &mut uuids[range.clone()],
                );
                chunk.dictionary.column(
                    required_string(&batch, "rel_type")?,
                    &mut rels[range.clone()],
                );
                for (name, ranks) in [("source_uuid", &mut *src), ("target_uuid", &mut *dst)] {
                    endpoints.clear();
                    endpoints.resize(count, [0_u8; 16]);
                    copy_uuids(
                        crate::graph_construction::batch_uuid_column(&batch, name)?,
                        &mut endpoints,
                    );
                    for (slot, endpoint) in ranks[range.clone()].iter_mut().zip(&endpoints) {
                        *slot = index.find(endpoint).unwrap_or_else(|| {
                            chunk.miss.get_or_insert(*endpoint);
                            0
                        });
                    }
                }
                written += count;
                if retain {
                    chunk.kept.push(batch);
                }
                Ok(())
            })?;
            if written != rows {
                return Err(short_source());
            }
            Ok(chunk)
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let miss = chunks.iter().find_map(|chunk| chunk.miss);
    let dictionaries = chunks
        .iter()
        .map(|chunk| &chunk.dictionary)
        .collect::<Vec<_>>();
    let (rel_names, maps) = global_dictionary(&dictionaries);
    remap_in_place(&tasks, &mut rels, &maps);
    let kept = chunks
        .into_iter()
        .flat_map(|chunk| chunk.kept)
        .collect::<Vec<_>>();
    if let Some(order) = sort_unique(&mut uuids, "edge")? {
        src = gather(&src, &order);
        dst = gather(&dst, &order);
        rels = gather(&rels, &order);
    }
    reject_node_collisions(&nodes.uuids, &uuids)?;
    if let Some(endpoint) = miss {
        return Err(storage(if uuids.binary_search(&endpoint).is_ok() {
            "edge endpoint is not a node UUID"
        } else {
            "edge endpoint UUID does not exist"
        }));
    }
    Ok(EdgeTable {
        uuids,
        src,
        dst,
        rels,
        rel_names,
        kept,
    })
}

/// A UUID names one node or one edge, never both.
fn reject_node_collisions(nodes: &[[u8; 16]], edges: &[[u8; 16]]) -> Result<(), GfError> {
    const CHUNK: usize = 1 << 20;
    let collided = edges.par_chunks(CHUNK).any(|chunk| {
        let mut position = nodes.partition_point(|node| node < &chunk[0]);
        for edge in chunk {
            while position < nodes.len() && nodes[position] < *edge {
                position += 1;
            }
            if position == nodes.len() {
                return false;
            }
            if nodes[position] == *edge {
                return true;
            }
        }
        false
    });
    if collided {
        return Err(storage(
            "duplicate identity across construction runs (edge UUID equals a node UUID)",
        ));
    }
    Ok(())
}

/// Ids of `values` in order of first appearance: the order the staged path
/// interns names from a UUID-ordered details family.
pub(super) fn first_appearance(values: &[u32], names: usize) -> Vec<u32> {
    let mut seen = vec![false; names];
    let mut order = Vec::with_capacity(names);
    for &value in values {
        if !seen[value as usize] {
            seen[value as usize] = true;
            order.push(value);
            if order.len() == names {
                break;
            }
        }
    }
    order
}
