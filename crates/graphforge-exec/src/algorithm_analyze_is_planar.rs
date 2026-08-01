use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
const NONE: i64 = -1;

/// One stored adjacency entry in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlanarityEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl PlanarityEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// Test planarity of the selected undirected simple projection.
pub(crate) fn is_planar(
    nodes: &[[u8; 16]],
    edges: &[PlanarityEdge],
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;
    let mut work = 0_usize;
    let index = index_nodes(nodes, control, &mut work)?;
    let adjacency = simple_adjacency(edges, &index, control, &mut work)?;
    if adjacency.len() <= 4 {
        return Ok(true);
    }
    let edge_count = adjacency.iter().try_fold(0_usize, |sum, neighbors| {
        sum.checked_add(neighbors.len())
            .ok_or_else(|| execution("is_planar edge count exceeds supported range"))
    })? / 2;
    if edge_count > adjacency.len().saturating_mul(3).saturating_sub(6) {
        return Ok(false);
    }
    LrState::new(adjacency, control, work).run()
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<[u8; 16], usize>, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut index = BTreeMap::new();
    for (position, uuid) in ordered.into_iter().enumerate() {
        checkpoint(control, work)?;
        if index.insert(uuid, position).is_some() {
            return Err(execution("is_planar node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn simple_adjacency(
    edges: &[PlanarityEdge],
    index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        if !index.contains_key(&edge.source) || !index.contains_key(&edge.target) {
            return Err(execution(
                "is_planar edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "is_planar edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut pairs = BTreeSet::new();
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = index[&edge.source];
        let target = index[&edge.target];
        if source != target {
            pairs.insert((source.min(target), source.max(target)));
        }
    }
    let mut adjacency = vec![Vec::new(); index.len()];
    for (source, target) in pairs {
        checkpoint(control, work)?;
        adjacency[source].push(target);
        adjacency[target].push(source);
    }
    Ok(adjacency)
}

#[derive(Clone, Copy)]
struct ConflictPair {
    left_low: i64,
    left_high: i64,
    right_low: i64,
    right_high: i64,
}

impl ConflictPair {
    const fn empty() -> Self {
        Self {
            left_low: NONE,
            left_high: NONE,
            right_low: NONE,
            right_high: NONE,
        }
    }

    fn swap(&mut self) {
        std::mem::swap(&mut self.left_low, &mut self.right_low);
        std::mem::swap(&mut self.left_high, &mut self.right_high);
    }

    const fn left_empty(self) -> bool {
        self.left_low == NONE && self.left_high == NONE
    }

    const fn right_empty(self) -> bool {
        self.right_low == NONE && self.right_high == NONE
    }
}

/// Iterative left-right planarity test over a deterministic DFS orientation.
///
/// This implements the conflict-pair criterion from Brandes, "The Left-Right
/// Planarity Test" (2012); only the Boolean decision is retained.
struct LrState<'a> {
    adjacency: Vec<Vec<usize>>,
    height: Vec<i64>,
    parent_edge: Vec<i64>,
    out_edges: Vec<Vec<usize>>,
    orientation_cursor: Vec<usize>,
    testing_cursor: Vec<usize>,
    edge_head: Vec<usize>,
    edge_tail: Vec<usize>,
    lowpoint: Vec<i64>,
    second_lowpoint: Vec<i64>,
    nesting_depth: Vec<i64>,
    reference: Vec<i64>,
    side: Vec<i8>,
    lowpoint_edge: Vec<i64>,
    stack_bottom: Vec<i64>,
    edge_id: HashMap<(usize, usize), usize>,
    oriented: HashSet<(usize, usize)>,
    orientation_resumed: HashSet<(usize, usize)>,
    testing_resumed: HashSet<usize>,
    roots: Vec<usize>,
    conflicts: Vec<ConflictPair>,
    control: &'a AlgorithmControl,
    work: usize,
}

impl<'a> LrState<'a> {
    fn new(adjacency: Vec<Vec<usize>>, control: &'a AlgorithmControl, work: usize) -> Self {
        let node_count = adjacency.len();
        Self {
            adjacency,
            height: vec![NONE; node_count],
            parent_edge: vec![NONE; node_count],
            out_edges: vec![Vec::new(); node_count],
            orientation_cursor: vec![0; node_count],
            testing_cursor: vec![0; node_count],
            edge_head: Vec::new(),
            edge_tail: Vec::new(),
            lowpoint: Vec::new(),
            second_lowpoint: Vec::new(),
            nesting_depth: Vec::new(),
            reference: Vec::new(),
            side: Vec::new(),
            lowpoint_edge: Vec::new(),
            stack_bottom: Vec::new(),
            edge_id: HashMap::new(),
            oriented: HashSet::new(),
            orientation_resumed: HashSet::new(),
            testing_resumed: HashSet::new(),
            roots: Vec::new(),
            conflicts: Vec::new(),
            control,
            work,
        }
    }

    fn run(mut self) -> Result<bool, AlgorithmError> {
        for node in 0..self.adjacency.len() {
            self.tick()?;
            if self.height[node] == NONE {
                self.height[node] = 0;
                self.roots.push(node);
                self.orient(node)?;
            }
        }
        for edges in &mut self.out_edges {
            edges.sort_by_key(|&edge| self.nesting_depth[edge]);
        }
        for index in 0..self.roots.len() {
            self.tick()?;
            if !self.test(self.roots[index])? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn new_edge(&mut self, tail: usize, head: usize) -> usize {
        let id = self.edge_tail.len();
        self.edge_tail.push(tail);
        self.edge_head.push(head);
        self.lowpoint.push(0);
        self.second_lowpoint.push(0);
        self.nesting_depth.push(0);
        self.reference.push(NONE);
        self.side.push(1);
        self.lowpoint_edge.push(NONE);
        self.stack_bottom.push(NONE);
        id
    }

    fn orient(&mut self, root: usize) -> Result<(), AlgorithmError> {
        let mut stack = vec![root];
        while let Some(node) = stack.pop() {
            self.tick()?;
            let parent = self.parent_edge[node];
            while self.orientation_cursor[node] < self.adjacency[node].len() {
                self.tick()?;
                let neighbor = self.adjacency[node][self.orientation_cursor[node]];
                let directed = (node, neighbor);
                let mut descended = false;
                if !self.orientation_resumed.contains(&directed) {
                    let undirected = (node.min(neighbor), node.max(neighbor));
                    if self.oriented.contains(&undirected) {
                        self.orientation_cursor[node] += 1;
                        continue;
                    }
                    self.oriented.insert(undirected);
                    let edge = self.new_edge(node, neighbor);
                    self.edge_id.insert(directed, edge);
                    self.out_edges[node].push(edge);
                    self.lowpoint[edge] = self.height[node];
                    self.second_lowpoint[edge] = self.height[node];
                    if self.height[neighbor] == NONE {
                        self.parent_edge[neighbor] = i64_index(edge)?;
                        self.height[neighbor] = self.height[node] + 1;
                        stack.push(node);
                        stack.push(neighbor);
                        self.orientation_resumed.insert(directed);
                        descended = true;
                    } else {
                        self.lowpoint[edge] = self.height[neighbor];
                    }
                }
                if descended {
                    break;
                }
                let edge = self.edge_id[&directed];
                self.nesting_depth[edge] = 2 * self.lowpoint[edge];
                if self.second_lowpoint[edge] < self.height[node] {
                    self.nesting_depth[edge] += 1;
                }
                if parent != NONE {
                    let parent = usize_index(parent);
                    match self.lowpoint[edge].cmp(&self.lowpoint[parent]) {
                        std::cmp::Ordering::Less => {
                            self.second_lowpoint[parent] =
                                self.lowpoint[parent].min(self.second_lowpoint[edge]);
                            self.lowpoint[parent] = self.lowpoint[edge];
                        }
                        std::cmp::Ordering::Greater => {
                            self.second_lowpoint[parent] =
                                self.second_lowpoint[parent].min(self.lowpoint[edge]);
                        }
                        std::cmp::Ordering::Equal => {
                            self.second_lowpoint[parent] =
                                self.second_lowpoint[parent].min(self.second_lowpoint[edge]);
                        }
                    }
                }
                self.orientation_cursor[node] += 1;
            }
        }
        Ok(())
    }

    fn test(&mut self, root: usize) -> Result<bool, AlgorithmError> {
        let mut stack = vec![root];
        while let Some(node) = stack.pop() {
            self.tick()?;
            let parent = self.parent_edge[node];
            let mut descended = false;
            while self.testing_cursor[node] < self.out_edges[node].len() {
                self.tick()?;
                let edge = self.out_edges[node][self.testing_cursor[node]];
                let head = self.edge_head[edge];
                if !self.testing_resumed.contains(&edge) {
                    self.stack_bottom[edge] = i64_index(self.conflicts.len())?;
                    if self.parent_edge[head] == i64_index(edge)? {
                        stack.push(node);
                        stack.push(head);
                        self.testing_resumed.insert(edge);
                        descended = true;
                        break;
                    }
                    self.lowpoint_edge[edge] = i64_index(edge)?;
                    self.conflicts.push(ConflictPair {
                        left_low: NONE,
                        left_high: NONE,
                        right_low: i64_index(edge)?,
                        right_high: i64_index(edge)?,
                    });
                }
                if self.lowpoint[edge] < self.height[node] {
                    if edge == self.out_edges[node][0] {
                        if parent != NONE {
                            self.lowpoint_edge[usize_index(parent)] = self.lowpoint_edge[edge];
                        }
                    } else if !self.add_constraints(edge, usize_index(parent))? {
                        return Ok(false);
                    }
                }
                self.testing_cursor[node] += 1;
            }
            if !descended && parent != NONE {
                self.remove_back_edges(usize_index(parent))?;
            }
        }
        Ok(true)
    }

    fn add_constraints(&mut self, edge: usize, parent: usize) -> Result<bool, AlgorithmError> {
        let mut pending = ConflictPair::empty();
        loop {
            self.tick()?;
            let mut pair = self
                .conflicts
                .pop()
                .ok_or_else(|| execution("is_planar conflict stack underflow"))?;
            if !pair.left_empty() {
                pair.swap();
            }
            if !pair.left_empty() {
                return Ok(false);
            }
            if self.lowpoint[usize_index(pair.right_low)] > self.lowpoint[parent] {
                if pending.right_empty() {
                    pending.right_high = pair.right_high;
                } else {
                    self.reference[usize_index(pending.right_low)] = pair.right_high;
                }
                pending.right_low = pair.right_low;
            } else {
                self.reference[usize_index(pair.right_low)] = self.lowpoint_edge[parent];
            }
            if i64_index(self.conflicts.len())? == self.stack_bottom[edge] {
                break;
            }
        }
        while let Some(&top) = self.conflicts.last() {
            self.tick()?;
            if !(self.left_conflicting(top, edge) || self.right_conflicting(top, edge)) {
                break;
            }
            let mut pair = self
                .conflicts
                .pop()
                .expect("conflict stack has a last entry");
            if self.right_conflicting(pair, edge) {
                pair.swap();
            }
            if self.right_conflicting(pair, edge) {
                return Ok(false);
            }
            if pending.right_low != NONE {
                self.reference[usize_index(pending.right_low)] = pair.right_high;
            }
            if pair.right_low != NONE {
                pending.right_low = pair.right_low;
            }
            if pending.left_empty() {
                pending.left_high = pair.left_high;
            } else {
                self.reference[usize_index(pending.left_low)] = pair.left_high;
            }
            pending.left_low = pair.left_low;
        }
        if !(pending.left_empty() && pending.right_empty()) {
            self.conflicts.push(pending);
        }
        Ok(true)
    }

    fn left_conflicting(&self, pair: ConflictPair, edge: usize) -> bool {
        pair.left_high != NONE && self.lowpoint[usize_index(pair.left_high)] > self.lowpoint[edge]
    }

    fn right_conflicting(&self, pair: ConflictPair, edge: usize) -> bool {
        pair.right_high != NONE && self.lowpoint[usize_index(pair.right_high)] > self.lowpoint[edge]
    }

    fn lowest(&self, pair: ConflictPair) -> i64 {
        if pair.left_empty() {
            self.lowpoint[usize_index(pair.right_low)]
        } else if pair.right_empty() {
            self.lowpoint[usize_index(pair.left_low)]
        } else {
            self.lowpoint[usize_index(pair.left_low)]
                .min(self.lowpoint[usize_index(pair.right_low)])
        }
    }

    fn remove_back_edges(&mut self, edge: usize) -> Result<(), AlgorithmError> {
        let tail = self.edge_tail[edge];
        while let Some(&top) = self.conflicts.last() {
            self.tick()?;
            if self.lowest(top) != self.height[tail] {
                break;
            }
            let pair = self.conflicts.pop().expect("conflict stack has an entry");
            if pair.left_low != NONE {
                self.side[usize_index(pair.left_low)] = -1;
            }
        }
        if let Some(mut pair) = self.conflicts.pop() {
            while pair.left_high != NONE && self.edge_head[usize_index(pair.left_high)] == tail {
                pair.left_high = self.reference[usize_index(pair.left_high)];
            }
            if pair.left_high == NONE && pair.left_low != NONE {
                self.reference[usize_index(pair.left_low)] = pair.right_low;
                self.side[usize_index(pair.left_low)] = -1;
                pair.left_low = NONE;
            }
            while pair.right_high != NONE && self.edge_head[usize_index(pair.right_high)] == tail {
                pair.right_high = self.reference[usize_index(pair.right_high)];
            }
            if pair.right_high == NONE && pair.right_low != NONE {
                self.reference[usize_index(pair.right_low)] = pair.left_low;
                self.side[usize_index(pair.right_low)] = -1;
                pair.right_low = NONE;
            }
            self.conflicts.push(pair);
        }
        if self.lowpoint[edge] >= self.height[tail] {
            return Ok(());
        }
        if let Some(top) = self.conflicts.last() {
            let left = top.left_high;
            let right = top.right_high;
            self.reference[edge] = if left != NONE
                && (right == NONE
                    || self.lowpoint[usize_index(left)] > self.lowpoint[usize_index(right)])
            {
                left
            } else {
                right
            };
        }
        Ok(())
    }

    fn tick(&mut self) -> Result<(), AlgorithmError> {
        checkpoint(self.control, &mut self.work)
    }
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn i64_index(value: usize) -> Result<i64, AlgorithmError> {
    i64::try_from(value).map_err(|_| execution("is_planar graph exceeds supported index range"))
}

fn usize_index(value: i64) -> usize {
    usize::try_from(value).expect("left-right edge reference is non-negative")
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8) -> PlanarityEdge {
        PlanarityEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn complete_bipartite(left: u8, right: u8) -> (Vec<[u8; 16]>, Vec<PlanarityEdge>) {
        let nodes = (0..left + right).map(uuid).collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut id = 32;
        for source in 0..left {
            for target in left..left + right {
                edges.push(edge(id, source, target));
                id += 1;
            }
        }
        (nodes, edges)
    }

    #[test]
    fn classifies_hand_verifiable_planar_and_non_planar_graphs() {
        let (k33_nodes, k33_edges) = complete_bipartite(3, 3);
        assert!(!is_planar(&k33_nodes, &k33_edges, &control()).unwrap());

        let nodes = (0..6).map(uuid).collect::<Vec<_>>();
        let forest = [
            edge(20, 0, 1),
            edge(21, 1, 2),
            edge(22, 3, 4),
            edge(23, 4, 5),
        ];
        assert!(is_planar(&nodes, &forest, &control()).unwrap());

        let petersen_nodes = (0..10).map(uuid).collect::<Vec<_>>();
        let petersen_edges = [
            edge(40, 0, 1),
            edge(41, 1, 2),
            edge(42, 2, 3),
            edge(43, 3, 4),
            edge(44, 4, 0),
            edge(45, 5, 7),
            edge(46, 7, 9),
            edge(47, 9, 6),
            edge(48, 6, 8),
            edge(49, 8, 5),
            edge(50, 0, 5),
            edge(51, 1, 6),
            edge(52, 2, 7),
            edge(53, 3, 8),
            edge(54, 4, 9),
        ];
        assert!(!is_planar(&petersen_nodes, &petersen_edges, &control()).unwrap());
    }

    #[test]
    fn empty_singleton_disconnected_and_multigraph_projection_are_planar() {
        assert!(is_planar(&[], &[], &control()).unwrap());
        assert!(is_planar(&[uuid(0)], &[], &control()).unwrap());
        let nodes = (0..5).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 2, 2),
            edge(14, 3, 4),
        ];
        assert!(is_planar(&nodes, &edges, &control()).unwrap());
    }

    #[test]
    fn identity_errors_and_controls_are_structured() {
        assert!(matches!(
            is_planar(&[uuid(0), uuid(0)], &[], &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            is_planar(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            is_planar(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control()
            ),
            Err(AlgorithmError::Execution { .. })
        ));

        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            is_planar(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            is_planar(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
