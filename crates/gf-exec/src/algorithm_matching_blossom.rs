use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_matching_state::{AlternatingLabel, ExactMatchingValue};
use std::collections::HashSet;
pub(crate) type LeafPath = (Vec<usize>, Vec<usize>);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum BlossomChild {
    Vertex(usize),
    Blossom(usize),
}
#[derive(Clone, Debug, PartialEq)]
struct Blossom {
    base: usize,
    children: Vec<BlossomChild>,
    connecting_edges: Vec<usize>,
    leaf_cycle: Option<LeafPath>,
    leaves: Vec<usize>,
    parent: Option<usize>,
    dual: ExactMatchingValue,
    active: bool,
}
#[derive(Clone)]
pub(crate) struct BlossomForest {
    vertex_owner: Vec<Option<usize>>,
    top_level: Vec<BlossomChild>,
    blossoms: Vec<Blossom>,
}
impl BlossomForest {
    pub(crate) fn new(vertex_count: usize) -> Result<Self, AlgorithmError> {
        Self::from_seed(vertex_count, &[], &[])
    }

    fn from_seed(
        vertex_count: usize,
        contractions: &[(usize, Vec<BlossomChild>, Vec<usize>)],
        expansions: &[usize],
    ) -> Result<Self, AlgorithmError> {
        let mut forest = Self {
            vertex_owner: vec![None; vertex_count],
            top_level: (0..vertex_count).map(BlossomChild::Vertex).collect(),
            blossoms: Vec::new(),
        };
        for (base, children, edges) in contractions {
            forest.contract(*base, children, edges)?;
        }
        for &blossom in expansions {
            forest.expand(blossom)?;
        }
        Ok(forest)
    }
    pub(crate) fn vertex_count(&self) -> usize {
        self.vertex_owner.len()
    }
    pub(crate) fn representative(&self, vertex: usize) -> Result<usize, AlgorithmError> {
        self.require_vertex(vertex)?;
        Ok(self.vertex_owner[vertex].map_or(vertex, |owner| self.blossoms[owner].base))
    }
    pub(crate) fn common_dual(
        &self,
        left: usize,
        right: usize,
    ) -> Result<ExactMatchingValue, AlgorithmError> {
        self.require_vertex(left)?;
        self.require_vertex(right)?;
        let mut dual = ExactMatchingValue::default();
        for blossom in self.blossoms.iter().filter(|blossom| {
            blossom.active && blossom.leaves.contains(&left) && blossom.leaves.contains(&right)
        }) {
            dual += &blossom.dual;
        }
        Ok(dual)
    }
    pub(crate) fn dual_bound(
        &self,
        labels: &[AlternatingLabel],
    ) -> Result<Option<ExactMatchingValue>, AlgorithmError> {
        self.validate()?;
        if labels.len() != self.vertex_owner.len() {
            return Err(execution("blossom labels must cover every vertex"));
        }
        Ok(self
            .blossoms
            .iter()
            .filter(|blossom| {
                blossom.active
                    && blossom.parent.is_none()
                    && labels[blossom.base] == AlternatingLabel::Inner
            })
            .map(|blossom| {
                let mut bound = blossom.dual.clone();
                bound >>= 1;
                bound
            })
            .min())
    }

    pub(crate) fn expand_zero_dual_invalid_matching(
        &mut self,
        mates: &[Option<usize>],
    ) -> Result<bool, AlgorithmError> {
        self.validate()?;
        if mates.len() != self.vertex_owner.len()
            || mates.iter().flatten().any(|&mate| mate >= mates.len())
        {
            return Err(execution("blossom matching must cover every vertex"));
        }
        let mut expanded = false;
        loop {
            let candidate = self.blossoms.iter().enumerate().find_map(|(id, blossom)| {
                if !blossom.active
                    || blossom.parent.is_some()
                    || blossom.dual != ExactMatchingValue::default()
                {
                    return None;
                }
                let crossings = blossom
                    .leaves
                    .iter()
                    .filter(|&&leaf| {
                        mates[leaf].is_some_and(|mate| !blossom.leaves.contains(&mate))
                    })
                    .count();
                let exposed = blossom
                    .leaves
                    .iter()
                    .filter(|&&leaf| mates[leaf].is_none())
                    .count();
                (crossings > 1 || exposed > 1 || (crossings > 0 && exposed > 0)).then_some(id)
            });
            let Some(id) = candidate else {
                break;
            };
            self.expand(id)?;
            expanded = true;
        }
        Ok(expanded)
    }

    pub(crate) fn apply_dual_step(
        &mut self,
        labels: &[AlternatingLabel],
        delta: &ExactMatchingValue,
        control: &AlgorithmControl,
    ) -> Result<Vec<LeafPath>, AlgorithmError> {
        self.validate()?;
        if labels.len() != self.vertex_owner.len() || delta < &ExactMatchingValue::default() {
            return Err(execution(
                "blossom dual update requires valid labels and delta",
            ));
        }
        let mut updated = self.clone();
        for blossom in &mut updated.blossoms {
            control.checkpoint()?;
            if !blossom.active || blossom.parent.is_some() {
                continue;
            }
            let mut doubled = delta.clone();
            doubled += delta;
            match labels[blossom.base] {
                AlternatingLabel::Outer => blossom.dual += &doubled,
                AlternatingLabel::Inner if doubled >= blossom.dual => {
                    blossom.dual = ExactMatchingValue::default();
                }
                AlternatingLabel::Inner => blossom.dual -= &doubled,
                AlternatingLabel::Free => {}
            }
            if blossom.dual < ExactMatchingValue::default() {
                return Err(execution("blossom dual update would violate feasibility"));
            }
        }
        let mut expansions = Vec::new();
        loop {
            let eligible = updated
                .blossoms
                .iter()
                .enumerate()
                .find_map(|(id, blossom)| {
                    (blossom.active
                        && blossom.parent.is_none()
                        && blossom.dual == ExactMatchingValue::default()
                        && labels[blossom.base] == AlternatingLabel::Inner)
                        .then_some(id)
                });
            let Some(id) = eligible else {
                break;
            };
            control.checkpoint()?;
            if let Some(cycle) = &updated.blossoms[id].leaf_cycle {
                expansions.push(cycle.clone());
            }
            updated.expand(id)?;
        }
        updated.validate()?;
        *self = updated;
        Ok(expansions)
    }
    pub(crate) fn check_complementary_slackness(
        &self,
        mates: &[Option<usize>],
    ) -> Result<(), AlgorithmError> {
        self.validate()?;
        if mates.len() != self.vertex_owner.len() {
            return Err(execution("blossom certificate must cover every vertex"));
        }
        for blossom in self
            .blossoms
            .iter()
            .filter(|blossom| blossom.active && blossom.dual > ExactMatchingValue::default())
        {
            let matched_inside = blossom
                .leaves
                .iter()
                .filter(|&&vertex| mates[vertex].is_some_and(|mate| blossom.leaves.contains(&mate)))
                .count()
                / 2;
            if matched_inside != (blossom.leaves.len() - 1) / 2 {
                return Err(execution(
                    "positive-dual blossom must have maximum internal cardinality",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn contract(
        &mut self,
        base: usize,
        children: &[BlossomChild],
        edges: &[usize],
    ) -> Result<usize, AlgorithmError> {
        if children.len() < 3 || children.len().is_multiple_of(2) || edges.len() != children.len() {
            return Err(execution(
                "blossom contraction requires an odd cycle and one edge per child",
            ));
        }
        self.require_vertex(base)?;
        let mut seen_children = HashSet::with_capacity(children.len());
        let mut seen_edges = HashSet::with_capacity(edges.len());
        let mut leaves = Vec::new();
        for &child in children {
            if !seen_children.insert(child) {
                return Err(execution("blossom cycle children must be unique"));
            }
            self.collect_available_leaves(child, &mut leaves)?;
        }
        if edges.iter().any(|edge| !seen_edges.insert(*edge)) {
            return Err(execution("blossom cycle edges must be unique"));
        }
        if !leaves.contains(&base) {
            return Err(execution("blossom base must belong to its cycle"));
        }
        let base_position = children
            .iter()
            .position(|&child| self.child_contains(child, base))
            .unwrap();
        let (children, edges) = canonical_cycle(children, edges, base_position);
        let id = self.blossoms.len();
        self.top_level.retain(|child| !children.contains(child));
        self.top_level.push(BlossomChild::Blossom(id));
        self.top_level.sort_unstable();
        for &child in &children {
            if let BlossomChild::Blossom(child_id) = child {
                self.blossoms[child_id].parent = Some(id);
            }
        }
        for &vertex in &leaves {
            self.vertex_owner[vertex] = Some(id);
        }
        self.blossoms.push(Blossom {
            base,
            children,
            connecting_edges: edges,
            leaf_cycle: None,
            leaves,
            parent: None,
            dual: ExactMatchingValue::default(),
            active: true,
        });
        self.validate()?;
        Ok(id)
    }
    pub(crate) fn contract_leaf_cycle(
        &mut self,
        base: usize,
        vertices: &[usize],
        edges: &[usize],
    ) -> Result<usize, AlgorithmError> {
        if vertices.len() != edges.len() || vertices.is_empty() {
            return Err(execution("leaf cycle requires one edge per vertex"));
        }
        let children = vertices
            .iter()
            .map(|&vertex| {
                self.require_vertex(vertex)?;
                Ok(self.vertex_owner[vertex]
                    .map_or(BlossomChild::Vertex(vertex), BlossomChild::Blossom))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        let mut cycle_children = Vec::new();
        let mut cycle_edges = Vec::new();
        for position in 0..children.len() {
            let next = (position + 1) % children.len();
            if children[position] != children[next] {
                cycle_children.push(children[position]);
                cycle_edges.push(edges[position]);
            }
        }
        let id = self.contract(base, &cycle_children, &cycle_edges)?;
        let base_position = vertices
            .iter()
            .position(|&vertex| vertex == base)
            .ok_or_else(|| execution("leaf cycle must contain its base"))?;
        self.blossoms[id].leaf_cycle = Some(canonical_cycle(vertices, edges, base_position));
        Ok(id)
    }
    pub(crate) fn lift_from_base(&self, vertex: usize) -> Result<Option<LeafPath>, AlgorithmError> {
        self.require_vertex(vertex)?;
        let Some(owner) = self.vertex_owner[vertex] else {
            return Ok(None);
        };
        Ok(Some(self.lift_in(owner, vertex)?))
    }
    fn lift_in(&self, owner: usize, vertex: usize) -> Result<LeafPath, AlgorithmError> {
        let (vertices, edges) = self.blossoms[owner]
            .leaf_cycle
            .as_ref()
            .ok_or_else(|| execution("active blossom requires a stored leaf cycle"))?;
        if let Some(position) = vertices.iter().position(|&candidate| candidate == vertex) {
            return if position.is_multiple_of(2) {
                Ok((vertices[..=position].to_vec(), edges[..position].to_vec()))
            } else {
                let mut path = vec![vertices[0]];
                path.extend(vertices[position..].iter().rev());
                let mut path_edges = vec![edges[edges.len() - 1]];
                path_edges.extend(edges[position..edges.len() - 1].iter().rev());
                Ok((path, path_edges))
            };
        }
        let child = self.blossoms[owner]
            .children
            .iter()
            .find_map(|child| match *child {
                BlossomChild::Blossom(id) if self.blossoms[id].leaves.contains(&vertex) => Some(id),
                _ => None,
            })
            .ok_or_else(|| execution("blossom leaf is absent from its stored hierarchy"))?;
        let mut prefix = self.lift_in(owner, self.blossoms[child].base)?;
        let suffix = self.lift_in(child, vertex)?;
        prefix.0.extend_from_slice(&suffix.0[1..]);
        prefix.1.extend(suffix.1);
        Ok(prefix)
    }
    fn expand(&mut self, id: usize) -> Result<Vec<BlossomChild>, AlgorithmError> {
        let blossom = self
            .blossoms
            .get(id)
            .ok_or_else(|| execution("blossom index is out of bounds"))?;
        if !blossom.active || blossom.parent.is_some() {
            return Err(execution("only an active top-level blossom can expand"));
        }
        let children = blossom.children.clone();
        self.blossoms[id].active = false;
        self.top_level
            .retain(|child| *child != BlossomChild::Blossom(id));
        self.top_level.extend_from_slice(&children);
        self.top_level.sort_unstable();
        for child in &children {
            match *child {
                BlossomChild::Vertex(vertex) => self.vertex_owner[vertex] = None,
                BlossomChild::Blossom(child_id) => {
                    self.blossoms[child_id].parent = None;
                    let mut leaves = Vec::new();
                    self.collect_leaves(child_id, &mut leaves);
                    for vertex in leaves {
                        self.vertex_owner[vertex] = Some(child_id);
                    }
                }
            }
        }
        self.validate()?;
        Ok(children)
    }
    fn collect_available_leaves(
        &self,
        child: BlossomChild,
        leaves: &mut Vec<usize>,
    ) -> Result<(), AlgorithmError> {
        match child {
            BlossomChild::Vertex(vertex) => {
                self.require_vertex(vertex)?;
                if self.vertex_owner[vertex].is_some() {
                    return Err(execution("blossom vertex already has an active owner"));
                }
                leaves.push(vertex);
            }
            BlossomChild::Blossom(id) => {
                self.blossoms
                    .get(id)
                    .filter(|blossom| blossom.active && blossom.parent.is_none())
                    .ok_or_else(|| execution("nested blossom must be active and top-level"))?;
                self.collect_leaves(id, leaves);
            }
        }
        Ok(())
    }
    fn collect_leaves(&self, id: usize, leaves: &mut Vec<usize>) {
        leaves.extend_from_slice(&self.blossoms[id].leaves);
    }
    fn child_contains(&self, child: BlossomChild, vertex: usize) -> bool {
        match child {
            BlossomChild::Vertex(candidate) => candidate == vertex,
            BlossomChild::Blossom(id) => self.blossoms[id].leaves.contains(&vertex),
        }
    }
    fn validate(&self) -> Result<(), AlgorithmError> {
        let mut seen_top = HashSet::new();
        if self.top_level.iter().any(|child| !seen_top.insert(*child)) {
            return Err(execution("top-level blossom children must be unique"));
        }
        for (id, blossom) in self.blossoms.iter().enumerate() {
            let top_level = self.top_level.contains(&BlossomChild::Blossom(id));
            if blossom.dual < ExactMatchingValue::default()
                || blossom.children.len() < 3
                || blossom.children.len().is_multiple_of(2)
                || blossom.children.len() != blossom.connecting_edges.len()
                || !self.child_contains(BlossomChild::Blossom(id), blossom.base)
            {
                return Err(execution("stored blossom invariant is invalid"));
            }
            if top_level != (blossom.active && blossom.parent.is_none()) {
                return Err(execution("top-level blossom state is inconsistent"));
            }
            if let Some(parent) = blossom.parent
                && self.blossoms.get(parent).is_none_or(|candidate| {
                    !candidate.active || !candidate.children.contains(&BlossomChild::Blossom(id))
                })
            {
                return Err(execution("nested blossom parent is inconsistent"));
            }
        }
        for (vertex, owner) in self.vertex_owner.iter().enumerate() {
            if self.top_level.contains(&BlossomChild::Vertex(vertex)) != owner.is_none() {
                return Err(execution("top-level vertex state is inconsistent"));
            }
            if let Some(owner) = owner
                && self.blossoms.get(*owner).is_none_or(|blossom| {
                    !blossom.active || !self.child_contains(BlossomChild::Blossom(*owner), vertex)
                })
            {
                return Err(execution("active blossom membership is inconsistent"));
            }
        }
        Ok(())
    }
    fn require_vertex(&self, vertex: usize) -> Result<(), AlgorithmError> {
        if vertex < self.vertex_owner.len() {
            Ok(())
        } else {
            Err(execution("blossom vertex index is out of bounds"))
        }
    }
}
fn canonical_cycle<T: Copy + Ord>(
    children: &[T],
    edges: &[usize],
    base_position: usize,
) -> (Vec<T>, Vec<usize>) {
    let len = children.len();
    let forward = (0..len)
        .map(|offset| children[(base_position + offset) % len])
        .collect::<Vec<_>>();
    let reverse = (0..len)
        .map(|offset| children[(base_position + len - offset) % len])
        .collect::<Vec<_>>();
    if forward <= reverse {
        let ordered_edges = (0..len)
            .map(|offset| edges[(base_position + offset) % len])
            .collect();
        (forward, ordered_edges)
    } else {
        let ordered_edges = (0..len)
            .map(|offset| edges[(base_position + len - offset - 1) % len])
            .collect();
        (reverse, ordered_edges)
    }
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

    fn vertices(values: &[usize]) -> Vec<BlossomChild> {
        values.iter().copied().map(BlossomChild::Vertex).collect()
    }
    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }
    fn dual(value: f64) -> ExactMatchingValue {
        ExactMatchingValue::from_weight(value)
    }
    fn rejected(forest: &mut BlossomForest, children: &[usize], edges: &[usize]) {
        assert!(forest.contract(0, &vertices(children), edges).is_err());
    }
    #[test]
    fn contracts_canonical_cycle_and_expands_in_stored_order() {
        let mut forest = BlossomForest::new(3).unwrap();
        forest
            .contract(1, &vertices(&[2, 1, 0]), &[20, 10, 30])
            .unwrap();
        assert_eq!(forest.blossoms[0].children, vertices(&[1, 0, 2]));
        assert_eq!(forest.blossoms[0].connecting_edges, [10, 30, 20]);
        assert_eq!(forest.expand(0).unwrap(), vertices(&[1, 0, 2]));
    }
    #[test]
    fn nests_and_restores_membership_while_rejecting_overlap() {
        let mut forest = BlossomForest::new(5).unwrap();
        let inner = forest
            .contract(0, &vertices(&[0, 1, 2]), &[0, 1, 2])
            .unwrap();
        let outer = forest
            .contract(
                0,
                &[
                    BlossomChild::Blossom(inner),
                    BlossomChild::Vertex(3),
                    BlossomChild::Vertex(4),
                ],
                &[3, 4, 5],
            )
            .unwrap();
        rejected(&mut forest, &[0, 3, 4], &[6, 7, 8]);
        forest.expand(outer).unwrap();
        assert_eq!(
            forest.vertex_owner,
            [Some(inner), Some(inner), Some(inner), None, None]
        );
        forest.expand(inner).unwrap();
        rejected(&mut forest, &[0, 1], &[0, 1]);
        rejected(&mut forest, &[0, 1, 2, 3], &[0, 1, 2, 3]);
        rejected(&mut forest, &[0, 1, 2], &[0, 0, 2]);
    }
    #[test]
    fn schedules_blossom_duals_and_cascades_zero_dual_expansion() {
        let mut forest = BlossomForest::new(5).unwrap();
        let child = forest
            .contract(0, &vertices(&[0, 1, 2]), &[10, 11, 12])
            .unwrap();
        let parent = forest
            .contract(
                0,
                &[
                    BlossomChild::Blossom(child),
                    BlossomChild::Vertex(3),
                    BlossomChild::Vertex(4),
                ],
                &[20, 21, 22],
            )
            .unwrap();
        forest.blossoms[parent].dual = dual(2.0);
        let mut labels = vec![AlternatingLabel::Free; 5];
        labels[0] = AlternatingLabel::Inner;
        assert_eq!(forest.dual_bound(&labels).unwrap(), Some(dual(1.0)));
        forest
            .apply_dual_step(&labels, &dual(1.0), &control())
            .unwrap();
        assert!(!forest.blossoms[parent].active);
        assert!(!forest.blossoms[child].active);
        assert_eq!(forest.top_level, vertices(&[0, 1, 2, 3, 4]));
        assert_eq!(forest.common_dual(0, 1).unwrap(), dual(0.0));
    }

    #[test]
    fn expands_only_zero_dual_blossoms_with_invalid_matching_boundaries() {
        let mut invalid = BlossomForest::new(5).unwrap();
        invalid
            .contract(4, &vertices(&[4, 0, 1]), &[40, 1, 14])
            .unwrap();
        let two_crossings_and_exposed = [Some(2), Some(3), Some(0), Some(1), None];
        assert!(
            invalid
                .expand_zero_dual_invalid_matching(&two_crossings_and_exposed)
                .unwrap()
        );
        assert_eq!(invalid.representative(0).unwrap(), 0);
        assert_eq!(invalid.representative(1).unwrap(), 1);
        assert_eq!(invalid.representative(4).unwrap(), 4);
        assert!(
            !invalid
                .expand_zero_dual_invalid_matching(&two_crossings_and_exposed)
                .unwrap()
        );

        let mut valid = BlossomForest::new(5).unwrap();
        valid
            .contract(4, &vertices(&[4, 0, 1]), &[40, 1, 14])
            .unwrap();
        let one_exposed = [Some(1), Some(0), None, None, None];
        assert!(
            !valid
                .expand_zero_dual_invalid_matching(&one_exposed)
                .unwrap()
        );
        assert_eq!(valid.representative(0).unwrap(), 4);
    }

    #[test]
    fn dual_updates_are_directional_atomic_and_checked() {
        let mut forest = BlossomForest::new(3).unwrap();
        let blossom = forest
            .contract(0, &vertices(&[0, 1, 2]), &[10, 11, 12])
            .unwrap();
        forest.blossoms[blossom].dual = dual(2.0);
        let mut labels = vec![AlternatingLabel::Free; 3];
        labels[0] = AlternatingLabel::Outer;
        forest
            .apply_dual_step(&labels, &dual(1.0), &control())
            .unwrap();
        assert_eq!(forest.blossoms[blossom].dual, dual(4.0));
        assert_eq!(forest.common_dual(0, 2).unwrap(), dual(4.0));

        let limits = AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        };
        assert!(
            forest
                .apply_dual_step(
                    &labels,
                    &dual(1.0),
                    &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                )
                .is_err()
        );
        assert_eq!(forest.blossoms[blossom].dual, dual(4.0));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(
            forest
                .apply_dual_step(
                    &labels,
                    &dual(1.0),
                    &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                )
                .is_err()
        );
        assert_eq!(forest.blossoms[blossom].dual, dual(4.0));
        assert!(
            forest
                .apply_dual_step(&labels, &dual(-1.0), &control())
                .is_err()
        );
    }
    #[test]
    fn subnormal_inner_dual_reaches_zero_without_stalling() {
        for dual_value in [f64::from_bits(1), f64::from_bits(3), f64::from_bits(5)] {
            let mut forest = BlossomForest::new(3).unwrap();
            let blossom = forest
                .contract(0, &vertices(&[0, 1, 2]), &[10, 11, 12])
                .unwrap();
            forest.blossoms[blossom].dual = dual(dual_value);
            let mut labels = vec![AlternatingLabel::Free; 3];
            labels[0] = AlternatingLabel::Inner;
            let bound = forest.dual_bound(&labels).unwrap().unwrap();
            assert!(bound > dual(0.0));
            let mut doubled = bound.clone();
            doubled += &bound;
            assert!(doubled >= dual(dual_value));
            forest.apply_dual_step(&labels, &bound, &control()).unwrap();
            assert!(!forest.blossoms[blossom].active);
        }
    }
    #[test]
    fn contracts_leaf_cycle_across_nested_child_boundaries() {
        let mut forest = BlossomForest::new(5).unwrap();
        let child = forest
            .contract_leaf_cycle(0, &[0, 1, 2], &[10, 11, 12])
            .unwrap();
        let parent = forest
            .contract_leaf_cycle(3, &[3, 0, 4], &[20, 21, 22])
            .unwrap();
        assert!(
            forest.blossoms[parent]
                .children
                .contains(&BlossomChild::Blossom(child))
        );
        assert_eq!(
            forest.lift_from_base(1).unwrap().unwrap().0,
            [3, 4, 0, 2, 1]
        );
    }
    #[test]
    fn certificate_requires_positive_dual_blossoms_to_be_saturated() {
        let mut forest = BlossomForest::new(3).unwrap();
        let blossom = forest
            .contract_leaf_cycle(0, &[0, 1, 2], &[10, 11, 12])
            .unwrap();
        forest.blossoms[blossom].dual = dual(2.0);
        assert!(
            forest
                .check_complementary_slackness(&[Some(1), Some(0), None])
                .is_ok()
        );
        assert!(
            forest
                .check_complementary_slackness(&[None, None, None])
                .is_err()
        );
    }
}
