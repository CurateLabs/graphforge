//! Pass 3: adjacency CSR shards, placed from the ranked edge arrays.
//!
//! An entry is `key << 32 | edge_id`. Sorting the entries of one direction
//! orders every node's list by `edge_id`, so each shard is a slice of the
//! sorted array and shards are encoded independently.

use rayon::prelude::*;

use super::emit::RelationRoute;
use super::tables::{EdgeTable, check_cancelled};
use super::{AtomicBool, GfError, Path, storage};
use crate::adjacency::{AdjacencyManifestRow, Direction};

pub(super) struct AdjacencyOutput {
    pub(super) captured: Vec<crate::adjacency::CapturedAdjacencyArtifact>,
    pub(super) shards: u64,
    pub(super) source_rows: u64,
}

fn sorted_entries(keys: &[u32]) -> Vec<u64> {
    let mut entries = keys
        .par_iter()
        .enumerate()
        .map(|(index, key)| (u64::from(*key) << 32) | (index as u64 + 1))
        .collect::<Vec<_>>();
    entries.par_sort_unstable();
    entries
}

#[allow(clippy::too_many_lines)]
pub(super) fn write_adjacency(
    graph_root: &Path,
    edges: &EdgeTable,
    relations: &[RelationRoute],
    generation: u64,
    built_at_micros: i64,
    allocation: Option<&crate::StorageAllocationOperation>,
    options: &crate::adjacency::AdjacencyBuildOptions,
    cancel: &AtomicBool,
) -> Result<AdjacencyOutput, GfError> {
    let adjacency = crate::adjacency::adjacency_dir(graph_root);
    if adjacency.exists() {
        std::fs::remove_dir_all(&adjacency).map_err(storage)?;
    }
    std::fs::create_dir_all(&adjacency).map_err(storage)?;

    // Relation groups in name order, then the union: the staged builder's order.
    let mut names = std::collections::BTreeMap::<&str, u32>::new();
    for relation in relations {
        let name = relation.adjacency_group();
        if crate::adjacency::usable_stem(name) {
            let next = u32::try_from(names.len()).map_err(storage)?;
            names.entry(name).or_insert(next);
        }
    }
    let total = edges.src.len();
    let mut ordered = names
        .keys()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    // `names` assigned ids in first-seen order; map each relation to its group's rank in name order.
    let rank = names
        .keys()
        .enumerate()
        .map(|(rank, name)| {
            (
                *name,
                u32::try_from(rank).expect("bounded by relation count"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let relation_group = relations
        .iter()
        .map(|relation| {
            let name = relation.adjacency_group();
            rank.get(name).copied().unwrap_or(u32::MAX)
        })
        .collect::<Vec<_>>();
    ordered.push(crate::adjacency::ALL_RELATIONS_STEM.to_owned());
    let union = ordered.len() - 1;

    let mut manifest = Vec::with_capacity(ordered.len() * 2);
    let mut output = AdjacencyOutput {
        captured: Vec::new(),
        shards: 0,
        source_rows: total as u64,
    };
    // One direction's sorted entries are resident at a time; outcomes are
    // collected by (stem, direction) and the manifest keeps the staged order.
    let mut outcomes = std::collections::BTreeMap::new();
    let covering = |group: usize| {
        relation_group
            .iter()
            .filter(|rank| **rank == u32::try_from(group).unwrap_or(u32::MAX))
            .count()
            == relations.len()
    };
    for (direction, keys, neighbors) in [
        (Direction::Out, &edges.src, &edges.dst),
        (Direction::In, &edges.dst, &edges.src),
    ] {
        let entries = sorted_entries(keys);
        for (group, stem) in ordered.iter().enumerate() {
            check_cancelled(cancel)?;
            let selected;
            let group_rank = u32::try_from(group).expect("bounded by relation count");
            let slice = if group == union || covering(group) {
                entries.as_slice()
            } else {
                selected = entries
                    .par_iter()
                    .filter(|entry| {
                        relation_group[edges.rels[(**entry & 0xffff_ffff) as usize - 1] as usize]
                            == group_rank
                    })
                    .copied()
                    .collect::<Vec<_>>();
                selected.as_slice()
            };
            outcomes.insert(
                (group, matches!(direction, Direction::In)),
                crate::adjacency::write_sharded_csr_from_sorted(
                    &crate::adjacency::csr_path(graph_root, stem, direction),
                    slice,
                    neighbors,
                    options.shard_max_edges,
                    options.shard_max_nodes,
                    allocation,
                )?,
            );
        }
    }
    for ((group, incoming), outcome) in outcomes {
        manifest.push(AdjacencyManifestRow {
            relation_type: ordered[group].clone(),
            direction: if incoming {
                Direction::In
            } else {
                Direction::Out
            },
            topology_generation: generation,
            built_at_micros,
            node_count: outcome.node_count,
            edge_count: outcome.edge_count,
        });
        output.shards += outcome.shards;
        output.captured.extend(outcome.captured);
    }
    crate::adjacency::write_manifest_observed(graph_root, &manifest, allocation)?;
    Ok(output)
}
