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

/// The CSR files an edge set produces: one per relation group in name order,
/// then the union of all relations (the staged builder's order).
pub(super) struct AdjacencyGroups {
    /// Group stems, relation groups first, the union last.
    pub(super) stems: Vec<String>,
    /// Group of every relation id, or `u32::MAX` when its edges are in the union only.
    pub(super) relation_group: Vec<u32>,
    /// Whether every edge belongs to the group, so its entries equal the union's.
    covering: Vec<bool>,
}

impl AdjacencyGroups {
    pub(super) fn new(relations: &[RelationRoute]) -> Result<Self, GfError> {
        let mut names = std::collections::BTreeSet::<&str>::new();
        for relation in relations {
            let name = relation.adjacency_group();
            if crate::adjacency::usable_stem(name) {
                names.insert(name);
            }
        }
        let mut stems = names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>();
        let rank = names
            .iter()
            .enumerate()
            .map(|(rank, name)| (*name, rank))
            .collect::<std::collections::BTreeMap<_, _>>();
        let relation_group = relations
            .iter()
            .map(|relation| {
                rank.get(relation.adjacency_group())
                    .map_or(Ok(u32::MAX), |rank| u32::try_from(*rank))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        stems.push(crate::adjacency::ALL_RELATIONS_STEM.to_owned());
        let covering = (0..stems.len())
            .map(|group| {
                relation_group
                    .iter()
                    .filter(|rank| **rank == u32::try_from(group).unwrap_or(u32::MAX))
                    .count()
                    == relations.len()
            })
            .collect();
        Ok(Self {
            stems,
            relation_group,
            covering,
        })
    }

    /// Index of the union group.
    pub(super) fn union(&self) -> usize {
        self.stems.len() - 1
    }

    /// Whether the group's entries are exactly the union's.
    pub(super) fn is_whole(&self, group: usize) -> bool {
        group == self.union() || self.covering[group]
    }
}

/// Write every group's manifest row, in the staged order, and gather the outcomes.
pub(super) fn assemble(
    graph_root: &Path,
    groups: &AdjacencyGroups,
    outcomes: std::collections::BTreeMap<(usize, bool), crate::adjacency::SortedCsrOutcome>,
    generation: u64,
    built_at_micros: i64,
    total_edges: u64,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<AdjacencyOutput, GfError> {
    let mut manifest = Vec::with_capacity(groups.stems.len() * 2);
    let mut output = AdjacencyOutput {
        captured: Vec::new(),
        shards: 0,
        source_rows: total_edges,
    };
    for ((group, incoming), outcome) in outcomes {
        manifest.push(AdjacencyManifestRow {
            relation_type: groups.stems[group].clone(),
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

/// A fresh adjacency directory below `graph_root`.
pub(super) fn reset_adjacency_directory(graph_root: &Path) -> Result<(), GfError> {
    let adjacency = crate::adjacency::adjacency_dir(graph_root);
    if adjacency.exists() {
        std::fs::remove_dir_all(&adjacency).map_err(storage)?;
    }
    std::fs::create_dir_all(&adjacency).map_err(storage)
}

#[allow(clippy::too_many_arguments)]
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
    reset_adjacency_directory(graph_root)?;
    let groups = AdjacencyGroups::new(relations)?;
    // One direction's sorted entries are resident at a time; outcomes are
    // collected by (stem, direction) and the manifest keeps the staged order.
    let mut outcomes = std::collections::BTreeMap::new();
    for (direction, keys, neighbors) in [
        (Direction::Out, &edges.src, &edges.dst),
        (Direction::In, &edges.dst, &edges.src),
    ] {
        let entries = sorted_entries(keys);
        for (group, stem) in groups.stems.iter().enumerate() {
            check_cancelled(cancel)?;
            let selected;
            let group_rank = u32::try_from(group).expect("bounded by relation count");
            let slice = if groups.is_whole(group) {
                entries.as_slice()
            } else {
                selected = entries
                    .par_iter()
                    .filter(|entry| {
                        groups.relation_group
                            [edges.rels[(**entry & 0xffff_ffff) as usize - 1] as usize]
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
    assemble(
        graph_root,
        &groups,
        outcomes,
        generation,
        built_at_micros,
        edges.src.len() as u64,
        allocation,
    )
}
