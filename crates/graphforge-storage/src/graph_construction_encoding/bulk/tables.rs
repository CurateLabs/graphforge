//! Passes 1 and 2: decode, validate, order and rank nodes and edges.
//!
//! Identifiers are dense `u32` ranks while the graph has fewer than 2^32 nodes
//! and edges. UUIDs exist only in the sorted arrays and at the output.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use arrow::array::Array;
use rayon::prelude::*;

use super::{
    BulkSource, ConstructionChunkKind, FixedSizeBinaryArray, GfError, RecordBatch, StringArray,
    required_string, storage,
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

    fn column(&mut self, values: &StringArray, out: &mut Vec<u32>) {
        let mut previous: Option<(&str, u32)> = None;
        for row in 0..values.len() {
            let value = values.value(row);
            let id = match previous {
                Some((text, id)) if text == value => id,
                _ => self.id(value),
            };
            previous = Some((value, id));
            out.push(id);
        }
    }
}

fn fixed_rows(array: &FixedSizeBinaryArray, out: &mut Vec<[u8; 16]>) {
    out.extend(
        array
            .value_data()
            .chunks_exact(16)
            .map(|bytes| <[u8; 16]>::try_from(bytes).expect("16-byte chunk")),
    );
}

fn concat<T: Copy + Send + Sync>(parts: &[&[T]], zero: T) -> Vec<T> {
    let total = parts.iter().map(|part| part.len()).sum();
    let mut out = vec![zero; total];
    let mut rest = out.as_mut_slice();
    let mut slices = Vec::with_capacity(parts.len());
    for part in parts {
        let (head, tail) = rest.split_at_mut(part.len());
        slices.push(head);
        rest = tail;
    }
    slices
        .into_par_iter()
        .zip(parts.par_iter())
        .for_each(|(destination, source)| destination.copy_from_slice(source));
    out
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

fn remap_to_global(dictionaries: &[LocalDictionary], columns: &mut [&mut Vec<u32>]) -> Vec<String> {
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
    columns
        .par_iter_mut()
        .zip(maps.par_iter())
        .for_each(|(column, map)| {
            for id in column.iter_mut() {
                *id = map[*id as usize];
            }
        });
    global.names
}

// ---------------------------------------------------------------- nodes

#[derive(Default)]
struct NodeChunk {
    uuids: Vec<[u8; 16]>,
    labels: Vec<u32>,
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
    cancel: &AtomicBool,
) -> Result<NodeTable, GfError> {
    let tasks = sources
        .iter()
        .enumerate()
        .flat_map(|(source, planned)| (0..planned.tasks).map(move |task| (source, task)))
        .collect::<Vec<_>>();
    let mut chunks = tasks
        .par_iter()
        .map(|&(source, task)| {
            check_cancelled(cancel)?;
            let mut chunk = NodeChunk::default();
            sources[source].reader.read_task(task, &mut |batch| {
                check_cancelled(cancel)?;
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Node,
                    &batch,
                )?;
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                fixed_rows(
                    crate::graph_construction::batch_uuid_column(&batch, "node_uuid")?,
                    &mut chunk.uuids,
                );
                chunk
                    .dictionary
                    .column(required_string(&batch, "label")?, &mut chunk.labels);
                if retain {
                    chunk.kept.push(batch);
                }
                Ok(())
            })?;
            Ok(chunk)
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let total = chunks.iter().map(|chunk| chunk.uuids.len() as u64).sum();
    require_dense(total, "nodes")?;
    let dictionaries = chunks
        .iter_mut()
        .map(|chunk| std::mem::take(&mut chunk.dictionary))
        .collect::<Vec<_>>();
    let mut label_columns = chunks
        .iter_mut()
        .map(|chunk| std::mem::take(&mut chunk.labels))
        .collect::<Vec<_>>();
    let label_names = remap_to_global(
        &dictionaries,
        &mut label_columns.iter_mut().collect::<Vec<_>>(),
    );
    let mut uuids = concat(
        &chunks
            .iter()
            .map(|chunk| chunk.uuids.as_slice())
            .collect::<Vec<_>>(),
        [0_u8; 16],
    );
    let mut labels = concat(
        &label_columns.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        0,
    );
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
    uuids: Vec<[u8; 16]>,
    src: Vec<u32>,
    dst: Vec<u32>,
    rels: Vec<u32>,
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
    nodes: &NodeTable,
    index: &NodeIndex<'_>,
    cancel: &AtomicBool,
) -> Result<EdgeTable, GfError> {
    let tasks = sources
        .iter()
        .enumerate()
        .flat_map(|(source, planned)| (0..planned.tasks).map(move |task| (source, task)))
        .collect::<Vec<_>>();
    let mut chunks = tasks
        .par_iter()
        .map(|&(source, task)| {
            check_cancelled(cancel)?;
            let mut chunk = EdgeChunk::default();
            sources[source].reader.read_task(task, &mut |batch| {
                check_cancelled(cancel)?;
                crate::graph_construction::validate_canonical_batch(
                    ConstructionChunkKind::Edge,
                    &batch,
                )?;
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let first = chunk.uuids.len();
                fixed_rows(
                    crate::graph_construction::batch_uuid_column(&batch, "edge_uuid")?,
                    &mut chunk.uuids,
                );
                chunk
                    .dictionary
                    .column(required_string(&batch, "rel_type")?, &mut chunk.rels);
                let mut endpoints = Vec::with_capacity(batch.num_rows());
                for (name, target) in [("source_uuid", false), ("target_uuid", true)] {
                    endpoints.clear();
                    fixed_rows(
                        crate::graph_construction::batch_uuid_column(&batch, name)?,
                        &mut endpoints,
                    );
                    for endpoint in &endpoints {
                        let rank = index.find(endpoint).unwrap_or_else(|| {
                            chunk.miss.get_or_insert(*endpoint);
                            0
                        });
                        if target {
                            chunk.dst.push(rank);
                        } else {
                            chunk.src.push(rank);
                        }
                    }
                }
                debug_assert_eq!(chunk.uuids.len() - first, batch.num_rows());
                if retain {
                    chunk.kept.push(batch);
                }
                Ok(())
            })?;
            Ok(chunk)
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let total = chunks.iter().map(|chunk| chunk.uuids.len() as u64).sum();
    require_dense(total, "edges")?;
    let miss = chunks.iter().find_map(|chunk| chunk.miss);
    let dictionaries = chunks
        .iter_mut()
        .map(|chunk| std::mem::take(&mut chunk.dictionary))
        .collect::<Vec<_>>();
    let mut rel_columns = chunks
        .iter_mut()
        .map(|chunk| std::mem::take(&mut chunk.rels))
        .collect::<Vec<_>>();
    let rel_names = remap_to_global(
        &dictionaries,
        &mut rel_columns.iter_mut().collect::<Vec<_>>(),
    );
    let mut uuids = concat(
        &chunks
            .iter()
            .map(|chunk| chunk.uuids.as_slice())
            .collect::<Vec<_>>(),
        [0_u8; 16],
    );
    let mut src = concat(
        &chunks
            .iter()
            .map(|chunk| chunk.src.as_slice())
            .collect::<Vec<_>>(),
        0,
    );
    let mut dst = concat(
        &chunks
            .iter()
            .map(|chunk| chunk.dst.as_slice())
            .collect::<Vec<_>>(),
        0,
    );
    let mut rels = concat(
        &rel_columns.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        0,
    );
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
