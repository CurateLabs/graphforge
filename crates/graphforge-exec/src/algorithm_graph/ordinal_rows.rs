//! Sorted, duplicate-free adjacency rows over ordinals in one flat array.
//!
//! Kernels that reason about the *set* of neighbors (clustering, label
//! propagation) build one of these from an export's ordinals instead of a
//! `Vec<Vec<_>>` or a per-node ordered map: two flat `u32` arrays, no map keyed
//! by a node, and no allocation per node (#1974).

use super::AdjacencyGraph;
use crate::algorithm_dispatch::AlgorithmError;

/// `offsets[n]..offsets[n + 1]` is the sorted, duplicate-free row of node `n`.
#[derive(Debug)]
pub(crate) struct OrdinalRows {
    offsets: Vec<u32>,
    targets: Vec<u32>,
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

fn overflow() -> AlgorithmError {
    execution("adjacency rows exceed the supported range")
}

impl OrdinalRows {
    /// Number of rows.
    pub(crate) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// The sorted row of `node`.
    pub(crate) fn row(&self, node: usize) -> &[u32] {
        &self.targets[self.offsets[node] as usize..self.offsets[node + 1] as usize]
    }

    /// Whether `target` is in the row of `source`.
    pub(crate) fn has_arc(&self, source: u32, target: u32) -> bool {
        self.row(source as usize).binary_search(&target).is_ok()
    }

    /// The distinct non-self out-neighbors of every node. `poll` runs once per
    /// adjacency entry read.
    ///
    /// # Errors
    /// Returns an execution error when an entry names an unselected node or the
    /// rows exceed `u32` offsets, and whatever `poll` returns.
    pub(crate) fn distinct_out(
        graph: &AdjacencyGraph,
        mut poll: impl FnMut() -> Result<(), AlgorithmError>,
    ) -> Result<Self, AlgorithmError> {
        let node_count = graph.node_count();
        let mut offsets = Vec::with_capacity(node_count + 1);
        offsets.push(0_u32);
        let mut targets: Vec<u32> = Vec::new();
        for source in 0..node_count {
            let row_start = targets.len();
            for &target in graph.neighbor_ordinals(source) {
                poll()?;
                if target as usize >= node_count {
                    return Err(execution("adjacency references an unselected node"));
                }
                if target as usize != source {
                    targets.push(target);
                }
            }
            targets[row_start..].sort_unstable();
            let kept = row_start + dedup_sorted(&mut targets[row_start..]);
            targets.truncate(kept);
            offsets.push(u32::try_from(targets.len()).map_err(|_| overflow())?);
        }
        Ok(Self { offsets, targets })
    }

    /// The rows with every arc reversed. Sources are visited in ascending
    /// order, so each reversed row is sorted.
    ///
    /// # Errors
    /// Returns an execution error when a node ordinal exceeds `u32`.
    pub(crate) fn transpose(&self) -> Result<Self, AlgorithmError> {
        let node_count = self.len();
        let mut offsets = vec![0_u32; node_count + 1];
        for &target in &self.targets {
            offsets[target as usize + 1] += 1;
        }
        for node in 0..node_count {
            offsets[node + 1] += offsets[node];
        }
        let mut cursor = offsets[..node_count].to_vec();
        let mut targets = vec![0_u32; self.targets.len()];
        for source in 0..node_count {
            let source_ordinal = u32::try_from(source).map_err(|_| overflow())?;
            for &target in self.row(source) {
                let slot = &mut cursor[target as usize];
                targets[*slot as usize] = source_ordinal;
                *slot += 1;
            }
        }
        Ok(Self { offsets, targets })
    }

    /// The union of every row with the matching row of `other`, deduplicated.
    ///
    /// # Errors
    /// Returns an execution error when the union exceeds `u32` offsets.
    pub(crate) fn union(&self, other: &Self) -> Result<Self, AlgorithmError> {
        let mut offsets = Vec::with_capacity(self.offsets.len());
        offsets.push(0_u32);
        let mut targets = Vec::with_capacity(self.targets.len() + other.targets.len());
        for node in 0..self.len() {
            let (mut left, mut right) = (self.row(node).iter(), other.row(node).iter());
            let (mut a, mut b) = (left.next(), right.next());
            while a.is_some() || b.is_some() {
                let next = match (a, b) {
                    (Some(&x), Some(&y)) if x == y => {
                        a = left.next();
                        b = right.next();
                        x
                    }
                    (Some(&x), Some(&y)) if x < y => {
                        a = left.next();
                        x
                    }
                    (Some(_) | None, Some(&y)) => {
                        b = right.next();
                        y
                    }
                    (Some(&x), None) => {
                        a = left.next();
                        x
                    }
                    (None, None) => break,
                };
                targets.push(next);
            }
            offsets.push(u32::try_from(targets.len()).map_err(|_| overflow())?);
        }
        Ok(Self { offsets, targets })
    }
}

/// Remove adjacent duplicates from a sorted slice, returning the kept length.
fn dedup_sorted(row: &mut [u32]) -> usize {
    let mut kept = 0;
    for index in 0..row.len() {
        if index == 0 || row[index] != row[kept - 1] {
            row[kept] = row[index];
            kept += 1;
        }
    }
    kept
}
