use std::cmp::Ordering;
use std::collections::VecDeque;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_matching_blossom::BlossomForest;
use crate::algorithm_weighted_undirected::WeightedEdge;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AlternatingLabel {
    Free,
    Outer,
    Inner,
}

#[derive(Clone, Debug, PartialEq)]
struct VertexState {
    mate: Option<(usize, usize)>,
    label: AlternatingLabel,
    parent: Option<(usize, usize)>,
    root: Option<usize>,
    dual: ExactMatchingValue,
}

#[derive(Clone)]
pub(crate) struct AlternatingDualState {
    blossoms: BlossomForest,
    vertices: Vec<VertexState>,
    outer_queue: VecDeque<usize>,
    queued: Vec<bool>,
}

#[derive(Clone, Copy)]
pub(crate) struct IndexedWeightedEdge {
    pub(crate) edge: usize,
    pub(crate) left: usize,
    pub(crate) right: usize,
    pub(crate) weight: f64,
}

#[derive(Debug, PartialEq)]
struct MatchingObjective {
    weight: ExactWeight,
    cardinality: usize,
    edges: Vec<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ExactWeight {
    sign: i8,
    limbs: Vec<u64>,
    scale: i64,
}

impl ExactWeight {
    fn one() -> Self {
        Self {
            sign: 1,
            limbs: vec![1],
            scale: 0,
        }
    }

    fn from_usize(value: usize) -> Self {
        if value == 0 {
            Self::default()
        } else {
            let zeros = value.trailing_zeros();
            Self {
                sign: 1,
                limbs: vec![u64::try_from(value >> zeros).unwrap()],
                scale: i64::from(zeros),
            }
        }
    }

    fn add(&mut self, value: f64) -> Result<(), AlgorithmError> {
        if !value.is_finite() {
            return Err(execution("matching objective weights must be finite"));
        }
        if value == 0.0 {
            return Ok(());
        }
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as usize;
        let significand =
            (bits & ((1_u64 << 52) - 1)) | u64::from(exponent != 0).wrapping_mul(1_u64 << 52);
        let mut term = Self {
            sign: if bits >> 63 == 0 { 1 } else { -1 },
            limbs: vec![significand],
            scale: if exponent == 0 {
                -1074
            } else {
                i64::try_from(exponent).unwrap() - 1023 - 52
            },
        };
        term.normalize();
        self.add_exact(&term);
        Ok(())
    }

    fn add_exact(&mut self, term: &Self) {
        if term.sign == 0 {
            return;
        }
        if self.sign == 0 {
            *self = term.clone();
            return;
        }
        let scale = self.scale.min(term.scale);
        let mut left = shifted_magnitude(&self.limbs, self.scale - scale);
        let right = shifted_magnitude(&term.limbs, term.scale - scale);
        if self.sign == term.sign {
            add_magnitude(&mut left, &right);
        } else {
            match compare_magnitude(&left, &right) {
                Ordering::Greater => subtract_magnitude(&mut left, &right),
                Ordering::Less => {
                    let mut magnitude = right;
                    subtract_magnitude(&mut magnitude, &left);
                    left = magnitude;
                    self.sign = term.sign;
                }
                Ordering::Equal => {
                    *self = Self::default();
                    return;
                }
            }
        }
        self.limbs = left;
        self.scale = scale;
        self.normalize();
    }

    fn normalize(&mut self) {
        trim(&mut self.limbs);
        if self.limbs.is_empty() {
            *self = Self::default();
            return;
        }
        let zero_limbs = self
            .limbs
            .iter()
            .position(|&limb| limb != 0)
            .unwrap_or(self.limbs.len());
        if zero_limbs > 0 {
            self.limbs.drain(..zero_limbs);
            self.scale += i64::try_from(zero_limbs * 64).unwrap();
        }
        let zeros = self.limbs[0].trailing_zeros() as usize;
        if zeros > 0 {
            shift_right(&mut self.limbs, zeros);
            self.scale += i64::try_from(zeros).unwrap();
        }
    }
}

impl std::ops::AddAssign<&Self> for ExactWeight {
    fn add_assign(&mut self, term: &Self) {
        self.add_exact(term);
    }
}

impl std::ops::SubAssign<&Self> for ExactWeight {
    fn sub_assign(&mut self, term: &Self) {
        let mut negative = term.clone();
        negative.sign = -negative.sign;
        self.add_exact(&negative);
    }
}

impl std::ops::ShrAssign<u32> for ExactWeight {
    fn shr_assign(&mut self, places: u32) {
        if self.sign != 0 {
            self.scale -= i64::from(places);
        }
    }
}

impl Ord for ExactWeight {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.sign.cmp(&other.sign) {
            Ordering::Equal if self.sign < 0 => compare_scaled(other, self),
            Ordering::Equal if self.sign > 0 => compare_scaled(self, other),
            ordering => ordering,
        }
    }
}

fn compare_scaled(left: &ExactWeight, right: &ExactWeight) -> Ordering {
    let scale = left.scale.min(right.scale);
    compare_magnitude(
        &shifted_magnitude(&left.limbs, left.scale - scale),
        &shifted_magnitude(&right.limbs, right.scale - scale),
    )
}

fn shifted_magnitude(value: &[u64], shift: i64) -> Vec<u64> {
    let shift = usize::try_from(shift).unwrap();
    let words = shift / 64;
    let bits = shift % 64;
    let mut result = vec![0; value.len() + words + usize::from(bits != 0)];
    for (position, &limb) in value.iter().enumerate() {
        result[position + words] |= limb << bits;
        if bits != 0 {
            result[position + words + 1] = limb >> (64 - bits);
        }
    }
    trim(&mut result);
    result
}

fn shift_right(value: &mut Vec<u64>, bits: usize) {
    debug_assert!(bits < 64);
    for position in 0..value.len() {
        value[position] = (value[position] >> bits)
            | value
                .get(position + 1)
                .copied()
                .unwrap_or(0)
                .wrapping_shl(u32::try_from(64 - bits).unwrap());
    }
    trim(value);
}

impl PartialOrd for ExactWeight {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExactMatchingValue {
    primary: ExactWeight,
    cardinality: ExactWeight,
    canonical: Vec<ExactWeight>,
}

impl ExactMatchingValue {
    #[cfg(test)]
    pub(crate) fn from_weight(weight: f64) -> Self {
        let mut primary = ExactWeight::default();
        primary.add(weight).unwrap();
        Self {
            primary,
            ..Self::default()
        }
    }

    pub(crate) fn from_edge(edge: &IndexedWeightedEdge) -> Result<Self, AlgorithmError> {
        let mut primary = ExactWeight::default();
        primary.add(edge.weight)?;
        let mut canonical = vec![ExactWeight::default(); edge.edge + 1];
        canonical[edge.edge] = ExactWeight::one();
        Ok(Self {
            primary,
            cardinality: ExactWeight::one(),
            canonical,
        })
    }

    fn from_objective(objective: &MatchingObjective) -> Self {
        let one = ExactWeight::one();
        let mut canonical = vec![
            ExactWeight::default();
            objective
                .edges
                .iter()
                .copied()
                .max()
                .map_or(0, |edge| edge + 1)
        ];
        for &edge in &objective.edges {
            canonical[edge] += &one;
        }
        Self {
            primary: objective.weight.clone(),
            cardinality: ExactWeight::from_usize(objective.cardinality),
            canonical,
        }
    }

    fn normalize(&mut self) {
        while self.canonical.last().is_some_and(|value| value.sign == 0) {
            self.canonical.pop();
        }
    }
}

impl std::ops::AddAssign<&Self> for ExactMatchingValue {
    fn add_assign(&mut self, term: &Self) {
        self.primary += &term.primary;
        self.cardinality += &term.cardinality;
        self.canonical.resize(
            self.canonical.len().max(term.canonical.len()),
            ExactWeight::default(),
        );
        for (left, right) in self.canonical.iter_mut().zip(&term.canonical) {
            *left += right;
        }
        self.normalize();
    }
}

impl std::ops::SubAssign<&Self> for ExactMatchingValue {
    fn sub_assign(&mut self, term: &Self) {
        self.primary -= &term.primary;
        self.cardinality -= &term.cardinality;
        self.canonical.resize(
            self.canonical.len().max(term.canonical.len()),
            ExactWeight::default(),
        );
        for (left, right) in self.canonical.iter_mut().zip(&term.canonical) {
            *left -= right;
        }
        self.normalize();
    }
}

impl std::ops::ShrAssign<u32> for ExactMatchingValue {
    fn shr_assign(&mut self, places: u32) {
        self.primary >>= places;
        self.cardinality >>= places;
        for value in &mut self.canonical {
            *value >>= places;
        }
    }
}

impl Ord for ExactMatchingValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.primary
            .cmp(&other.primary)
            .then_with(|| self.cardinality.cmp(&other.cardinality))
            .then_with(|| {
                let zero = ExactWeight::default();
                (0..self.canonical.len().max(other.canonical.len()))
                    .map(|position| {
                        self.canonical
                            .get(position)
                            .unwrap_or(&zero)
                            .cmp(other.canonical.get(position).unwrap_or(&zero))
                    })
                    .find(|ordering| ordering.is_ne())
                    .unwrap_or(Ordering::Equal)
            })
    }
}

impl PartialOrd for ExactMatchingValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn compare_magnitude(left: &[u64], right: &[u64]) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.iter().rev().cmp(right.iter().rev()))
}

fn add_magnitude(left: &mut Vec<u64>, right: &[u64]) {
    left.resize(left.len().max(right.len()), 0);
    let mut carry = false;
    for (position, &value) in right.iter().enumerate() {
        let (sum, first) = left[position].overflowing_add(value);
        let (sum, second) = sum.overflowing_add(u64::from(carry));
        left[position] = sum;
        carry = first || second;
    }
    let mut position = right.len();
    while carry && position < left.len() {
        (left[position], carry) = left[position].overflowing_add(1);
        position += 1;
    }
    if carry {
        left.push(1);
    }
}

fn subtract_magnitude(left: &mut Vec<u64>, right: &[u64]) {
    let mut borrow = false;
    for (position, value) in right
        .iter()
        .copied()
        .chain(std::iter::repeat(0))
        .take(left.len())
        .enumerate()
    {
        let (difference, first) = left[position].overflowing_sub(value);
        let (difference, second) = difference.overflowing_sub(u64::from(borrow));
        left[position] = difference;
        borrow = first || second;
    }
    debug_assert!(!borrow);
    trim(left);
}

fn trim(value: &mut Vec<u64>) {
    while value.last() == Some(&0) {
        value.pop();
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AlternatingPath {
    vertices: Vec<usize>,
    edges: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TightEdgeAction {
    Grow {
        outer: usize,
        free: usize,
        edge: usize,
    },
    Contract {
        left: usize,
        right: usize,
        edge: usize,
    },
    Augment {
        left: usize,
        right: usize,
        edge: usize,
    },
}

impl AlternatingDualState {
    pub(crate) fn new(node_count: usize, edges: &[WeightedEdge]) -> Result<Self, AlgorithmError> {
        Self::from_seed(node_count, edges, &[], &[])
    }

    fn from_seed(
        node_count: usize,
        edges: &[WeightedEdge],
        mates: &[(usize, usize, usize)],
        extensions: &[(usize, usize, usize)],
    ) -> Result<Self, AlgorithmError> {
        let dual = edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| edge.source_uuid != edge.target_uuid)
            .try_fold(ExactMatchingValue::default(), |dual, (edge, value)| {
                let value = ExactMatchingValue::from_edge(&IndexedWeightedEdge {
                    edge,
                    left: 0,
                    right: 0,
                    weight: value.weight,
                })?;
                Ok::<_, AlgorithmError>(dual.max(value))
            })?;
        let blossoms = BlossomForest::new(node_count)?;
        let mut state = Self {
            blossoms,
            vertices: vec![
                VertexState {
                    mate: None,
                    label: AlternatingLabel::Free,
                    parent: None,
                    root: None,
                    dual,
                };
                node_count
            ],
            outer_queue: VecDeque::new(),
            queued: vec![false; node_count],
        };
        for &(left, right, edge) in mates {
            state.mate_pair(left, right, edge)?;
        }
        for vertex in 0..node_count {
            if state.vertices[vertex].mate.is_none() {
                state.start_root(vertex)?;
            }
        }
        for &(outer, inner, edge) in extensions {
            state.extend(outer, inner, edge)?;
        }
        for &(vertices, edges) in &[] as &[(&[usize], &[usize])] {
            state.augment(vertices, edges)?;
        }
        for &(certificate, objective) in &[] as &[(&[IndexedWeightedEdge], &MatchingObjective)] {
            state.check_vertex_optimality(certificate, Some(objective))?;
        }
        for &(edges, control) in &[] as &[(&[IndexedWeightedEdge], &AlgorithmControl)] {
            state.dual_step(edges, control, &mut 0)?;
            state.solve_exact(edges, control)?;
        }
        for &(left, right, edge, control) in &[] as &[(usize, usize, usize, &AlgorithmControl)] {
            state.contract_tight_cycle(left, right, edge, control)?;
            state.augment_tight_paths(left, right, edge, control)?;
        }
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn mate_pair(
        &mut self,
        left: usize,
        right: usize,
        edge: usize,
    ) -> Result<(), AlgorithmError> {
        self.require_vertex(left)?;
        self.require_vertex(right)?;
        if left == right
            || self.vertices[left].mate.is_some()
            || self.vertices[right].mate.is_some()
        {
            return Err(execution("matching mates must be distinct and free"));
        }
        self.vertices[left].mate = Some((right, edge));
        self.vertices[right].mate = Some((left, edge));
        Ok(())
    }

    pub(crate) fn start_root(&mut self, vertex: usize) -> Result<(), AlgorithmError> {
        self.require_vertex(vertex)?;
        if self.blossoms.representative(vertex)? != vertex || self.crossing_mate(vertex)?.is_some()
        {
            return Err(execution(
                "alternating root must be an exposed representative",
            ));
        }
        self.start_validated_root(vertex)
    }

    fn start_validated_root(&mut self, vertex: usize) -> Result<(), AlgorithmError> {
        let state = &mut self.vertices[vertex];
        if state.label != AlternatingLabel::Free {
            return Err(execution("alternating root must be free and unlabeled"));
        }
        state.label = AlternatingLabel::Outer;
        state.root = Some(vertex);
        self.enqueue(vertex);
        Ok(())
    }

    pub(crate) fn extend(
        &mut self,
        outer: usize,
        inner: usize,
        edge: usize,
    ) -> Result<usize, AlgorithmError> {
        self.require_vertex(outer)?;
        self.require_vertex(inner)?;
        let outer_representative = self.blossoms.representative(outer)?;
        let inner_representative = self.blossoms.representative(inner)?;
        let root = self.vertices[outer_representative]
            .root
            .filter(|_| self.vertices[outer_representative].label == AlternatingLabel::Outer)
            .ok_or_else(|| execution("alternating parent must be outer"))?;
        if self.vertices[inner_representative].label != AlternatingLabel::Free {
            return Err(execution("alternating child must be unlabeled"));
        }
        let (matched_inner, mate, matched_edge) = self
            .crossing_mate(inner_representative)?
            .ok_or_else(|| execution("inner alternating blossom must be matched"))?;
        let mate_representative = self.blossoms.representative(mate)?;
        if self.vertices[mate_representative].label != AlternatingLabel::Free {
            return Err(execution("matched outer vertex must be unlabeled"));
        }
        let (path, path_edges) = self
            .blossoms
            .lift_from_base(inner)?
            .unwrap_or_else(|| (vec![inner], Vec::new()));
        for (position, &vertex) in path.iter().enumerate() {
            self.vertices[vertex].label = if position.is_multiple_of(2) {
                AlternatingLabel::Inner
            } else {
                AlternatingLabel::Outer
            };
            self.vertices[vertex].root = Some(root);
            self.vertices[vertex].parent = path
                .get(position + 1)
                .map(|&parent| (parent, path_edges[position]));
        }
        self.vertices[inner].parent = Some((outer, edge));
        self.vertices[mate_representative].label = AlternatingLabel::Outer;
        self.vertices[mate_representative].parent = Some((matched_inner, matched_edge));
        self.vertices[mate_representative].root = Some(root);
        self.enqueue(mate_representative);
        Ok(mate_representative)
    }

    fn crossing_mate(
        &self,
        representative: usize,
    ) -> Result<Option<(usize, usize, usize)>, AlgorithmError> {
        self.require_vertex(representative)?;
        let mut crossing = None;
        for vertex in 0..self.vertices.len() {
            if self.blossoms.representative(vertex)? != representative {
                continue;
            }
            let Some((mate, edge)) = self.vertices[vertex].mate else {
                continue;
            };
            if self.blossoms.representative(mate)? != representative {
                if crossing.is_some() {
                    return Err(execution("alternating blossom has multiple crossing mates"));
                }
                crossing = Some((vertex, mate, edge));
            }
        }
        Ok(crossing)
    }

    pub(crate) fn pop_outer(&mut self) -> Option<usize> {
        debug_assert_eq!(self.blossoms.vertex_count(), self.vertices.len());
        let vertex = self.outer_queue.pop_front()?;
        self.queued[vertex] = false;
        Some(vertex)
    }

    pub(crate) fn slack(
        &self,
        edge: &IndexedWeightedEdge,
    ) -> Result<ExactMatchingValue, AlgorithmError> {
        self.require_vertex(edge.left)?;
        self.require_vertex(edge.right)?;
        if !edge.weight.is_finite() {
            return Err(execution("matching slack requires finite weight"));
        }
        let value = ExactMatchingValue::from_edge(edge)?;
        let mut slack = self.vertices[edge.left].dual.clone();
        slack += &self.vertices[edge.right].dual;
        slack -= &value;
        slack -= &value;
        slack += &self.blossoms.common_dual(edge.left, edge.right)?;
        Ok(slack)
    }

    fn augment(&mut self, path: &[usize], edges: &[usize]) -> Result<(), AlgorithmError> {
        if path.len() != edges.len().saturating_add(1)
            || edges.len().is_multiple_of(2)
            || path.len() < 2
        {
            return Err(execution("augmenting path must have odd edge length"));
        }
        let mut seen = vec![false; self.vertices.len()];
        for &vertex in path {
            self.require_vertex(vertex)?;
            if std::mem::replace(&mut seen[vertex], true) {
                return Err(execution("augmenting path vertices must be unique"));
            }
        }
        if self.vertices[path[0]].mate.is_some()
            || self.vertices[*path.last().unwrap()].mate.is_some()
        {
            return Err(execution("augmenting path endpoints must be free"));
        }
        for (position, pair) in path.windows(2).enumerate() {
            let current = self.vertices[pair[0]].mate;
            if position.is_multiple_of(2) {
                if current == Some((pair[1], edges[position])) {
                    return Err(execution("augmenting path must alternate unmatched edges"));
                }
            } else if current != Some((pair[1], edges[position]))
                || self.vertices[pair[1]].mate != Some((pair[0], edges[position]))
            {
                return Err(execution(
                    "augmenting path matched edge identity is invalid",
                ));
            }
        }
        for position in (1..edges.len()).step_by(2) {
            self.vertices[path[position]].mate = None;
            self.vertices[path[position + 1]].mate = None;
        }
        for position in (0..edges.len()).step_by(2) {
            self.vertices[path[position]].mate = Some((path[position + 1], edges[position]));
            self.vertices[path[position + 1]].mate = Some((path[position], edges[position]));
        }
        self.reset_forest()?;
        self.validate()
    }

    fn root_path(
        &self,
        outer: usize,
        control: &AlgorithmControl,
    ) -> Result<AlternatingPath, AlgorithmError> {
        self.require_vertex(outer)?;
        let representative = self.blossoms.representative(outer)?;
        if self.vertices[representative].label != AlternatingLabel::Outer {
            return Err(execution("alternating path endpoint must be outer"));
        }
        if let Some((lifted_vertices, lifted_edges)) = self.blossoms.lift_from_base(outer)? {
            let base = lifted_vertices[0];
            if base != outer {
                let mut path = self.root_path(base, control)?;
                for _ in &lifted_edges {
                    control.checkpoint()?;
                }
                path.vertices.extend_from_slice(&lifted_vertices[1..]);
                path.edges.extend(lifted_edges);
                return Ok(path);
            }
        }
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        let mut seen = vec![false; self.vertices.len()];
        let mut current = outer;
        loop {
            control.checkpoint()?;
            if std::mem::replace(&mut seen[current], true) {
                return Err(execution("alternating parent path contains a cycle"));
            }
            vertices.push(current);
            let Some((parent, edge)) = self.vertices[current].parent else {
                break;
            };
            self.require_vertex(parent)?;
            edges.push(edge);
            current = parent;
        }
        vertices.reverse();
        edges.reverse();
        Ok(AlternatingPath { vertices, edges })
    }

    fn contract_tight_cycle(
        &mut self,
        left: usize,
        right: usize,
        edge: usize,
        control: &AlgorithmControl,
    ) -> Result<usize, AlgorithmError> {
        let left_path = self.root_path(left, control)?;
        let right_path = self.root_path(right, control)?;
        let common = left_path
            .vertices
            .iter()
            .zip(&right_path.vertices)
            .take_while(|(left, right)| left == right)
            .count();
        let left_representative = self.blossoms.representative(left)?;
        let right_representative = self.blossoms.representative(right)?;
        if common == 0
            || self.vertices[left_representative].root != self.vertices[right_representative].root
            || left_representative == right_representative
        {
            return Err(execution("blossom endpoints require one shared root"));
        }
        let common_representative = self
            .blossoms
            .representative(left_path.vertices[common - 1])?;
        let mut base_position = common - 1;
        while base_position > 0
            && self
                .blossoms
                .representative(left_path.vertices[base_position - 1])?
                == common_representative
        {
            base_position -= 1;
        }
        let mut vertices = left_path.vertices[base_position..].to_vec();
        vertices.extend(right_path.vertices[common..].iter().rev());
        let mut edges = left_path.edges[base_position..].to_vec();
        edges.push(edge);
        edges.extend(right_path.edges[common - 1..].iter().rev());
        control.check_cancelled()?;
        let mut blossoms = self.blossoms.clone();
        let id = blossoms.contract_leaf_cycle(vertices[0], &vertices, &edges)?;
        control.check_cancelled()?;
        self.blossoms = blossoms;
        Ok(id)
    }

    fn augment_tight_paths(
        &mut self,
        left: usize,
        right: usize,
        edge: usize,
        control: &AlgorithmControl,
    ) -> Result<(), AlgorithmError> {
        let left_path = self.root_path(left, control)?;
        let right_path = self.root_path(right, control)?;
        let left_representative = self.blossoms.representative(left)?;
        let right_representative = self.blossoms.representative(right)?;
        if self.vertices[left_representative].root == self.vertices[right_representative].root {
            return Err(execution("augmentation endpoints require distinct roots"));
        }
        let mut vertices = left_path.vertices;
        vertices.extend(right_path.vertices.iter().rev());
        let mut edges = left_path.edges;
        edges.push(edge);
        edges.extend(right_path.edges.iter().rev());
        control.check_cancelled()?;
        self.augment(&vertices, &edges)
    }

    fn objective(
        &self,
        edges: &[IndexedWeightedEdge],
    ) -> Result<MatchingObjective, AlgorithmError> {
        let mut selected = Vec::new();
        let mut symbolic = ExactMatchingValue::default();
        for (vertex, state) in self.vertices.iter().enumerate() {
            let Some((mate, edge_id)) = state.mate else {
                continue;
            };
            if vertex > mate {
                continue;
            }
            let edge = edges
                .iter()
                .find(|edge| {
                    edge.edge == edge_id
                        && ((edge.left, edge.right) == (vertex, mate)
                            || (edge.left, edge.right) == (mate, vertex))
                })
                .ok_or_else(|| execution("matched edge identity is absent"))?;
            symbolic += &ExactMatchingValue::from_edge(edge)?;
            selected.push(edge_id);
        }
        selected.sort_unstable();
        Ok(MatchingObjective {
            weight: symbolic.primary,
            cardinality: selected.len(),
            edges: selected,
        })
    }

    fn compare_objectives(left: &MatchingObjective, right: &MatchingObjective) -> Ordering {
        ExactMatchingValue::from_objective(left).cmp(&ExactMatchingValue::from_objective(right))
    }

    fn check_vertex_optimality(
        &self,
        edges: &[IndexedWeightedEdge],
        expected: Option<&MatchingObjective>,
    ) -> Result<(), AlgorithmError> {
        self.validate()?;
        let objective = self.objective(edges)?;
        if expected.is_some_and(|expected| Self::compare_objectives(&objective, expected).is_ne()) {
            return Err(execution("matching objective certificate disagrees"));
        }
        for edge in edges {
            if !edge.weight.is_finite() {
                return Err(execution("optimality certificate weights must be finite"));
            }
            let slack = self.slack(edge)?;
            if slack < ExactMatchingValue::default() {
                return Err(execution("matching dual certificate is infeasible"));
            }
            let matched = self.vertices[edge.left].mate == Some((edge.right, edge.edge))
                || self.vertices[edge.right].mate == Some((edge.left, edge.edge));
            if matched && slack != ExactMatchingValue::default() {
                return Err(execution("matched edges must be dual-tight"));
            }
        }
        if self
            .vertices
            .iter()
            .any(|vertex| vertex.mate.is_none() && vertex.dual != ExactMatchingValue::default())
        {
            return Err(execution("exposed vertices must have zero dual"));
        }
        let mates = self
            .vertices
            .iter()
            .map(|vertex| vertex.mate.map(|(mate, _)| mate))
            .collect::<Vec<_>>();
        self.blossoms.check_complementary_slackness(&mates)?;
        Ok(())
    }

    fn reset_forest(&mut self) -> Result<(), AlgorithmError> {
        self.outer_queue.clear();
        self.queued.fill(false);
        for vertex in &mut self.vertices {
            vertex.label = AlternatingLabel::Free;
            vertex.parent = None;
            vertex.root = None;
        }
        let mates = self
            .vertices
            .iter()
            .map(|vertex| vertex.mate.map(|(mate, _)| mate))
            .collect::<Vec<_>>();
        self.blossoms.expand_zero_dual_invalid_matching(&mates)?;
        let representatives = (0..self.vertices.len())
            .map(|vertex| self.blossoms.representative(vertex))
            .collect::<Result<Vec<_>, _>>()?;
        let mut has_crossing_mate = vec![false; self.vertices.len()];
        for (vertex, state) in self.vertices.iter().enumerate() {
            let Some((mate, _)) = state.mate else {
                continue;
            };
            self.require_vertex(mate)?;
            if representatives[vertex] != representatives[mate] {
                has_crossing_mate[representatives[vertex]] = true;
            }
        }
        for vertex in 0..self.vertices.len() {
            if representatives[vertex] == vertex && !has_crossing_mate[vertex] {
                self.start_validated_root(vertex)?;
            }
        }
        Ok(())
    }

    fn retire_zero_dual_roots(&mut self) -> Result<bool, AlgorithmError> {
        let mut retired = vec![false; self.vertices.len()];
        for (vertex, state) in self.vertices.iter().enumerate() {
            retired[vertex] =
                state.root == Some(vertex) && state.dual == ExactMatchingValue::default();
        }
        if !retired.iter().any(|&root| root) {
            return Ok(false);
        }
        let mut remaining_queue = VecDeque::new();
        for &vertex in &self.outer_queue {
            let root = self.vertices[vertex]
                .root
                .ok_or_else(|| execution("queued alternating vertex must have a root"))?;
            if !retired
                .get(root)
                .copied()
                .ok_or_else(|| execution("alternating root is out of bounds"))?
            {
                remaining_queue.push_back(vertex);
            }
        }
        self.outer_queue = remaining_queue;
        self.queued.fill(false);
        for &vertex in &self.outer_queue {
            self.queued[vertex] = true;
        }
        for vertex in &mut self.vertices {
            let should_retire = match vertex.root {
                Some(root) => retired
                    .get(root)
                    .copied()
                    .ok_or_else(|| execution("alternating root is out of bounds"))?,
                None => false,
            };
            if should_retire {
                vertex.label = AlternatingLabel::Free;
                vertex.parent = None;
                vertex.root = None;
            }
        }
        Ok(true)
    }

    fn tight_edge_action(
        &self,
        edges: &[IndexedWeightedEdge],
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<Option<TightEdgeAction>, AlgorithmError> {
        let mut best = None;
        for edge in edges {
            matching_checkpoint(control, work)?;
            self.require_vertex(edge.left)?;
            self.require_vertex(edge.right)?;
            if edge.left == edge.right || self.slack(edge)? != ExactMatchingValue::default() {
                continue;
            }
            let left_representative = self.blossoms.representative(edge.left)?;
            let right_representative = self.blossoms.representative(edge.right)?;
            if left_representative == right_representative {
                continue;
            }
            if self.vertices[edge.left].mate == Some((edge.right, edge.edge))
                || self.vertices[edge.right].mate == Some((edge.left, edge.edge))
            {
                continue;
            }
            let (left, right) = match (
                self.vertices[left_representative].label,
                self.vertices[right_representative].label,
            ) {
                (AlternatingLabel::Outer, AlternatingLabel::Free | AlternatingLabel::Outer) => {
                    (edge.left, edge.right)
                }
                (AlternatingLabel::Free, AlternatingLabel::Outer) => (edge.right, edge.left),
                _ => continue,
            };
            let left_representative = self.blossoms.representative(left)?;
            let right_representative = self.blossoms.representative(right)?;
            let action = if self.vertices[right_representative].label == AlternatingLabel::Free {
                TightEdgeAction::Grow {
                    outer: left,
                    free: right,
                    edge: edge.edge,
                }
            } else if self.vertices[left_representative].root
                == self.vertices[right_representative].root
            {
                TightEdgeAction::Contract {
                    left,
                    right,
                    edge: edge.edge,
                }
            } else {
                TightEdgeAction::Augment {
                    left,
                    right,
                    edge: edge.edge,
                }
            };
            if best.is_none_or(|(_, best_edge)| edge.edge < best_edge) {
                best = Some((action, edge.edge));
            }
        }
        Ok(best.map(|(action, _)| action))
    }

    fn dual_step(
        &mut self,
        edges: &[IndexedWeightedEdge],
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<(ExactMatchingValue, Option<TightEdgeAction>), AlgorithmError> {
        let mut updated = self.clone();
        let result = updated.dual_step_inner(edges, control, work)?;
        updated.validate()?;
        *self = updated;
        Ok(result)
    }

    fn dual_step_inner(
        &mut self,
        edges: &[IndexedWeightedEdge],
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<(ExactMatchingValue, Option<TightEdgeAction>), AlgorithmError> {
        control.check_cancelled()?;
        let representatives = (0..self.vertices.len())
            .map(|vertex| self.blossoms.representative(vertex))
            .collect::<Result<Vec<_>, _>>()?;
        let labels = representatives
            .iter()
            .map(|&representative| self.vertices[representative].label)
            .collect::<Vec<_>>();
        let mut delta = (0..self.vertices.len())
            .filter(|&vertex| labels[vertex] == AlternatingLabel::Outer)
            .map(|vertex| self.vertices[vertex].dual.clone())
            .min()
            .ok_or_else(|| execution("dual update requires an outer vertex"))?;
        if let Some(bound) = self.blossoms.dual_bound(&labels)? {
            delta = delta.min(bound);
        }
        for edge in edges {
            matching_checkpoint(control, work)?;
            self.require_vertex(edge.left)?;
            self.require_vertex(edge.right)?;
            let left_representative = self.blossoms.representative(edge.left)?;
            let right_representative = self.blossoms.representative(edge.right)?;
            if left_representative == right_representative {
                continue;
            }
            let slack = self.slack(edge)?;
            if slack < ExactMatchingValue::default() {
                return Err(execution("dual update requires feasible input"));
            }
            let candidate = match (
                self.vertices[left_representative].label,
                self.vertices[right_representative].label,
            ) {
                (AlternatingLabel::Outer, AlternatingLabel::Free)
                | (AlternatingLabel::Free, AlternatingLabel::Outer) => Some(slack),
                (AlternatingLabel::Outer, AlternatingLabel::Outer) => {
                    let mut half = slack;
                    half >>= 1;
                    Some(half)
                }
                _ => None,
            };
            if let Some(candidate) = candidate {
                delta = delta.min(candidate);
            }
        }
        if delta < ExactMatchingValue::default() {
            return Err(execution(
                "dual update delta must be finite and nonnegative",
            ));
        }
        let mut blossoms = self.blossoms.clone();
        let expansions = blossoms.apply_dual_step(&labels, &delta, control)?;
        let duals: Vec<ExactMatchingValue> = self
            .vertices
            .iter()
            .zip(&labels)
            .map(|(vertex, label)| {
                let mut dual = vertex.dual.clone();
                match label {
                    AlternatingLabel::Outer => dual -= &delta,
                    AlternatingLabel::Inner => dual += &delta,
                    AlternatingLabel::Free => {}
                }
                dual
            })
            .collect();
        if duals
            .iter()
            .any(|dual| dual < &ExactMatchingValue::default())
        {
            return Err(execution("dual update would invalidate vertex duals"));
        }
        self.blossoms = blossoms;
        for (vertex, dual) in self.vertices.iter_mut().zip(duals) {
            vertex.dual = dual;
        }
        if edges.iter().any(|edge| {
            !self
                .slack(edge)
                .is_ok_and(|slack| slack >= ExactMatchingValue::default())
        }) {
            return Err(execution("dual update could not preserve feasibility"));
        }
        control.check_cancelled()?;
        self.reconstruct_expansions(&expansions)?;
        Ok((delta, self.tight_edge_action(edges, control, work)?))
    }

    pub(crate) fn solve_exact(
        &mut self,
        edges: &[IndexedWeightedEdge],
        control: &AlgorithmControl,
    ) -> Result<Vec<usize>, AlgorithmError> {
        let mut updated = self.clone();
        let selected = updated.solve_exact_inner(edges, control)?;
        *self = updated;
        Ok(selected)
    }

    fn solve_exact_inner(
        &mut self,
        edges: &[IndexedWeightedEdge],
        control: &AlgorithmControl,
    ) -> Result<Vec<usize>, AlgorithmError> {
        let mut work = 0;
        let mut rebuilt_zero_delta_forest = false;
        loop {
            control.checkpoint()?;
            let representatives = (0..self.vertices.len())
                .map(|vertex| self.blossoms.representative(vertex))
                .collect::<Result<Vec<_>, _>>()?;
            let has_outer = representatives.iter().any(|&representative| {
                self.vertices[representative].label == AlternatingLabel::Outer
            });
            if !has_outer {
                self.check_vertex_optimality(edges, None)?;
                return Ok(self.objective(edges)?.edges);
            }
            let action = if let Some(action) = self.tight_edge_action(edges, control, &mut work)? {
                action
            } else {
                let (delta, action) = self.dual_step(edges, control, &mut work)?;
                match action {
                    Some(action) => action,
                    None if delta > ExactMatchingValue::default() => {
                        rebuilt_zero_delta_forest = false;
                        continue;
                    }
                    None if self.retire_zero_dual_roots()? => {
                        rebuilt_zero_delta_forest = false;
                        continue;
                    }
                    None if !rebuilt_zero_delta_forest && self.reset_forest_if_changed()? => {
                        rebuilt_zero_delta_forest = true;
                        continue;
                    }
                    None => {
                        self.check_vertex_optimality(edges, None)?;
                        return Ok(self.objective(edges)?.edges);
                    }
                }
            };
            match action {
                TightEdgeAction::Grow {
                    outer, free, edge, ..
                } => {
                    self.extend(outer, free, edge)?;
                }
                TightEdgeAction::Contract {
                    left, right, edge, ..
                } => {
                    self.contract_tight_cycle(left, right, edge, control)?;
                }
                TightEdgeAction::Augment {
                    left, right, edge, ..
                } => self.augment_tight_paths(left, right, edge, control)?,
            }
            rebuilt_zero_delta_forest = false;
        }
    }

    fn reset_forest_if_changed(&mut self) -> Result<bool, AlgorithmError> {
        let before = self
            .vertices
            .iter()
            .map(|vertex| (vertex.label, vertex.parent, vertex.root))
            .collect::<Vec<_>>();
        let queue_before = self.outer_queue.clone();
        let membership_before = self.queued.clone();
        self.reset_forest()?;
        Ok(before
            != self
                .vertices
                .iter()
                .map(|vertex| (vertex.label, vertex.parent, vertex.root))
                .collect::<Vec<_>>()
            || queue_before != self.outer_queue
            || membership_before != self.queued)
    }

    fn reconstruct_expansions(
        &mut self,
        expansions: &[(Vec<usize>, Vec<usize>)],
    ) -> Result<(), AlgorithmError> {
        let mut covered = vec![false; self.vertices.len()];
        for (vertices, _) in expansions {
            for &vertex in vertices {
                self.require_vertex(vertex)?;
                covered[vertex] = true;
            }
        }
        self.outer_queue.retain(|&vertex| !covered[vertex]);
        self.queued
            .iter_mut()
            .zip(&covered)
            .for_each(|(queued, covered)| *queued &= !covered);
        covered.fill(false);
        for (vertices, edges) in expansions {
            if vertices.iter().any(|&vertex| covered[vertex]) {
                continue;
            }
            let (vertices, edges) = self.expanded_tree_path(vertices, edges)?;
            let entry = vertices[0];
            let root = self.vertices[entry]
                .root
                .ok_or_else(|| execution("expanded inner blossom requires a rooted entry"))?;
            let parent = self.vertices[entry].parent;
            for (position, &vertex) in vertices.iter().enumerate() {
                covered[vertex] = true;
                self.vertices[vertex].label = if position.is_multiple_of(2) {
                    AlternatingLabel::Inner
                } else {
                    AlternatingLabel::Outer
                };
                self.vertices[vertex].root = Some(root);
                self.vertices[vertex].parent = if position == 0 {
                    parent
                } else {
                    Some((vertices[position - 1], edges[position - 1]))
                };
                self.queued[vertex] = false;
            }
            let external_mates = vertices
                .iter()
                .filter_map(|&vertex| {
                    self.vertices[vertex]
                        .mate
                        .filter(|(mate, _)| !vertices.contains(mate))
                        .map(|(mate, edge)| (vertex, mate, edge))
                })
                .collect::<Vec<_>>();
            for (inner, outer, edge) in external_mates {
                self.vertices[outer].label = AlternatingLabel::Outer;
                self.vertices[outer].parent = Some((inner, edge));
                self.vertices[outer].root = Some(root);
                self.enqueue(outer);
            }
            for &vertex in vertices.iter().skip(1).step_by(2) {
                self.enqueue(vertex);
            }
        }
        Ok(())
    }

    fn expanded_tree_path(
        &self,
        vertices: &[usize],
        edges: &[usize],
    ) -> Result<(Vec<usize>, Vec<usize>), AlgorithmError> {
        if vertices.len() != edges.len() || vertices.is_empty() {
            return Err(execution("expanded blossom cycle is malformed"));
        }
        let mut entries = vertices
            .iter()
            .enumerate()
            .filter_map(|(position, &vertex)| {
                self.vertices[vertex]
                    .parent
                    .is_some_and(|(parent, _)| !vertices.contains(&parent))
                    .then_some(position)
            });
        let entry = entries
            .next()
            .ok_or_else(|| execution("expanded inner blossom requires one crossing parent"))?;
        if entries.next().is_some() {
            return Err(execution(
                "expanded inner blossom has multiple crossing parents",
            ));
        }
        let (mate, matched_edge) = self.vertices[vertices[entry]]
            .mate
            .ok_or_else(|| execution("expanded inner blossom entry must be matched"))?;
        let len = vertices.len();
        let forward = ((entry + 1) % len, edges[entry]);
        let reverse = ((entry + len - 1) % len, edges[(entry + len - 1) % len]);
        let entry_vertex = vertices[entry];
        let parent_direction = (
            self.vertices[vertices[forward.0]].parent == Some((entry_vertex, forward.1)),
            self.vertices[vertices[reverse.0]].parent == Some((entry_vertex, reverse.1)),
        );
        let preferred_direction = match parent_direction {
            (true, false) => Some(1_isize),
            (false, true) => Some(-1),
            (true, true) => None,
            (false, false) if (vertices[forward.0], forward.1) == (mate, matched_edge) => Some(1),
            (false, false) if (vertices[reverse.0], reverse.1) == (mate, matched_edge) => Some(-1),
            (false, false) => {
                return Err(execution("expanded blossom entry has no cycle direction"));
            }
        };
        let preserves_matching = |direction: isize| {
            let positions = if direction > 0 {
                (entry..len).chain(0..entry).collect::<Vec<_>>()
            } else {
                (0..=entry).rev().chain((entry + 1..len).rev()).collect()
            };
            let mut position_by_vertex = vec![None; self.vertices.len()];
            for (position, &cycle_position) in positions.iter().enumerate() {
                position_by_vertex[vertices[cycle_position]] = Some(position);
            }
            positions
                .iter()
                .enumerate()
                .all(|(position, &cycle_position)| {
                    let vertex = vertices[cycle_position];
                    let Some((mate, _)) = self.vertices[vertex].mate else {
                        return !position.is_multiple_of(2);
                    };
                    position_by_vertex[mate].map_or(position.is_multiple_of(2), |mate_position| {
                        position.is_multiple_of(2) != mate_position.is_multiple_of(2)
                    })
                })
        };
        let forward_valid = preserves_matching(1);
        let reverse_valid = preserves_matching(-1);
        let direction =
            select_expansion_direction(preferred_direction, forward_valid, reverse_valid)?;
        let positions = if direction > 0 {
            (entry..len).chain(0..entry).collect::<Vec<_>>()
        } else {
            (0..=entry).rev().chain((entry + 1..len).rev()).collect()
        };
        let tree_edges = positions
            .windows(2)
            .map(|pair| {
                if direction > 0 {
                    edges[pair[0]]
                } else {
                    edges[pair[1]]
                }
            })
            .collect();
        Ok((
            positions
                .iter()
                .map(|&position| vertices[position])
                .collect(),
            tree_edges,
        ))
    }

    fn validate(&self) -> Result<(), AlgorithmError> {
        let mut seen_queue = vec![false; self.vertices.len()];
        for &vertex in &self.outer_queue {
            self.require_vertex(vertex)?;
            if seen_queue[vertex]
                || !self.queued[vertex]
                || self.vertices[vertex].label != AlternatingLabel::Outer
            {
                return Err(execution("alternating outer queue is inconsistent"));
            }
            seen_queue[vertex] = true;
        }
        for (vertex, state) in self.vertices.iter().enumerate() {
            if self.queued[vertex] != seen_queue[vertex] {
                return Err(execution("alternating queue membership is inconsistent"));
            }
            if let Some((mate, edge)) = state.mate
                && self
                    .vertices
                    .get(mate)
                    .is_none_or(|peer| peer.mate != Some((vertex, edge)))
            {
                return Err(execution("matching mate relation must be symmetric"));
            }
            let coherent = match state.label {
                AlternatingLabel::Free => state.parent.is_none() && state.root.is_none(),
                AlternatingLabel::Outer => state.root.is_some(),
                AlternatingLabel::Inner => state.parent.is_some() && state.root.is_some(),
            };
            if !coherent {
                return Err(execution("alternating label, parent, and root disagree"));
            }
        }
        Ok(())
    }

    fn enqueue(&mut self, vertex: usize) {
        if !self.queued[vertex] {
            self.queued[vertex] = true;
            self.outer_queue.push_back(vertex);
        }
    }

    fn require_vertex(&self, vertex: usize) -> Result<(), AlgorithmError> {
        if vertex < self.vertices.len() {
            Ok(())
        } else {
            Err(execution("matching vertex index is out of bounds"))
        }
    }
}

fn matching_checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(4_096) {
        control.checkpoint().map(|_| ())
    } else {
        control.check_cancelled()
    }
}

fn select_expansion_direction(
    preferred: Option<isize>,
    forward_valid: bool,
    reverse_valid: bool,
) -> Result<isize, AlgorithmError> {
    match (preferred, forward_valid, reverse_valid) {
        (Some(1), true, _) | (Some(-1) | None, true, false) => Ok(1),
        (Some(-1), _, true) | (Some(1) | None, false, true) => Ok(-1),
        (None, true, true) => Err(execution("expanded blossom parent direction is ambiguous")),
        _ => Err(execution(
            "expanded blossom directions conflict with matching parity",
        )),
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

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> WeightedEdge {
        WeightedEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }

    fn indexed(edge: usize, left: usize, right: usize, weight: f64) -> IndexedWeightedEdge {
        IndexedWeightedEdge {
            edge,
            left,
            right,
            weight,
        }
    }

    fn control(cancellation: AlgorithmCancellation) -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
    }

    fn exact(values: &[f64]) -> ExactWeight {
        let mut sum = ExactWeight::default();
        for &value in values {
            sum.add(value).unwrap();
        }
        sum
    }
    fn value(weight: f64) -> ExactMatchingValue {
        ExactMatchingValue::from_weight(weight)
    }
    fn doubled(mut value: ExactMatchingValue) -> ExactMatchingValue {
        let copy = value.clone();
        value += &copy;
        value
    }

    #[test]
    fn exact_objective_weight_is_order_invariant_and_overflow_safe() {
        let adjacent = 1.0_f64.next_up();
        let values = [f64::MAX, f64::MAX, -f64::MAX, adjacent, -1.0];
        let forward = exact(&values);
        let reverse = exact(&values.into_iter().rev().collect::<Vec<_>>());
        assert_eq!(forward, reverse);
        assert!(forward > ExactWeight::default());
        assert_eq!(exact(&[f64::MAX, f64::MAX]), exact(&[f64::MAX; 2]));
        assert!(exact(&[f64::MAX, f64::MAX]) > exact(&[f64::MAX]));
        assert!(exact(&[-f64::MAX; 2]) < exact(&[-f64::MAX]));
        assert_eq!(exact(&[f64::MIN_POSITIVE, -f64::MIN_POSITIVE]), exact(&[]));
    }

    #[test]
    fn matching_state_boundaries_are_exact_and_queue_is_idempotent() {
        let mut state = AlternatingDualState::new(3, &[]).unwrap();
        assert!(state.require_vertex(0).is_ok());
        assert_eq!(
            state.require_vertex(3).unwrap_err(),
            execution("matching vertex index is out of bounds")
        );
        state.outer_queue.clear();
        state.queued.fill(false);
        state.enqueue(1);
        state.enqueue(1);
        assert_eq!(state.outer_queue.iter().copied().collect::<Vec<_>>(), [1]);
        assert!(state.queued[1]);

        assert_eq!(
            state.mate_pair(1, 1, 0).unwrap_err(),
            execution("matching mates must be distinct and free")
        );
        state.mate_pair(0, 1, 7).unwrap();
        assert_eq!(state.vertices[0].mate, Some((1, 7)));
        assert_eq!(state.vertices[1].mate, Some((0, 7)));
        assert_eq!(
            state.mate_pair(0, 2, 8).unwrap_err(),
            execution("matching mates must be distinct and free")
        );

        for (preferred, forward, reverse, expected) in [
            (Some(1), true, true, Ok(1)),
            (Some(-1), true, false, Ok(1)),
            (None, true, false, Ok(1)),
            (Some(-1), false, true, Ok(-1)),
            (Some(1), false, true, Ok(-1)),
        ] {
            assert_eq!(
                select_expansion_direction(preferred, forward, reverse),
                expected
            );
        }
        assert_eq!(
            select_expansion_direction(None, true, true).unwrap_err(),
            execution("expanded blossom parent direction is ambiguous")
        );
        assert_eq!(
            select_expansion_direction(None, false, false).unwrap_err(),
            execution("expanded blossom directions conflict with matching parity")
        );
    }

    #[test]
    fn exact_dyadic_halves_below_subnormal_and_normalizes_cancellation() {
        let smallest = exact(&[f64::from_bits(1)]);
        let mut half = smallest.clone();
        half >>= 1;
        assert!(half > ExactWeight::default());
        assert!(half < smallest);
        let mut restored = half.clone();
        restored.add_exact(&half);
        assert_eq!(restored, smallest);

        let mut deep = smallest.clone();
        deep >>= 256;
        assert!(deep > ExactWeight::default());
        let mut cancelled = exact(&[1.0]);
        cancelled.add_exact(&deep);
        cancelled -= &deep;
        assert_eq!(cancelled, exact(&[1.0]));
    }

    #[test]
    fn exact_dyadic_matches_independent_common_scale_oracle() {
        for left_scale in -8..=8 {
            for right_scale in -8..=8 {
                let left = ExactWeight {
                    sign: 1,
                    limbs: vec![3],
                    scale: left_scale,
                };
                let right = ExactWeight {
                    sign: -1,
                    limbs: vec![5],
                    scale: right_scale,
                };
                let common = left_scale.min(right_scale);
                let oracle = (3_i128 << (left_scale - common)) - (5_i128 << (right_scale - common));
                let mut actual = left.clone();
                actual.add_exact(&right);
                assert_eq!(
                    actual.cmp(&ExactWeight::default()),
                    oracle.cmp(&0),
                    "left_scale={left_scale} right_scale={right_scale}"
                );
                let mut reversed = right;
                reversed.add_exact(&left);
                assert_eq!(actual, reversed);
            }
        }
    }

    #[test]
    fn exact_objective_orders_cardinality_then_canonical_edges() {
        let objective = |cardinality, edges| MatchingObjective {
            weight: exact(&[3.0, -1.0]),
            cardinality,
            edges,
        };
        assert!(
            AlternatingDualState::compare_objectives(
                &objective(2, vec![2, 9]),
                &objective(1, vec![1])
            )
            .is_gt()
        );
        assert!(
            AlternatingDualState::compare_objectives(
                &objective(2, vec![2, 8]),
                &objective(2, vec![2, 9])
            )
            .is_gt()
        );
    }

    #[test]
    fn symbolic_objective_arithmetic_is_closed_across_canonical_lengths() {
        let objective = |weight: f64, edges: Vec<usize>| MatchingObjective {
            weight: exact(&[weight]),
            cardinality: edges.len(),
            edges,
        };
        let left = ExactMatchingValue::from_objective(&objective(f64::from_bits(1), vec![1, 7]));
        let right = ExactMatchingValue::from_objective(&objective(-2.0, vec![3]));
        let mut combined = left.clone();
        combined += &right;
        combined -= &right;
        assert_eq!(combined, left);

        let mut half = left.clone();
        half >>= 1;
        let mut restored = half.clone();
        restored += &half;
        assert_eq!(restored, left);
        assert_eq!(
            ExactMatchingValue::from_objective(&objective(1.0, vec![7, 1])),
            ExactMatchingValue::from_objective(&objective(1.0, vec![1, 7]))
        );
    }

    #[test]
    fn symbolic_edges_encode_cardinality_and_canonical_parallel_order() {
        let earlier = ExactMatchingValue::from_edge(&indexed(2, 0, 1, f64::from_bits(1))).unwrap();
        let later = ExactMatchingValue::from_edge(&indexed(3, 0, 1, f64::from_bits(1))).unwrap();
        assert!(earlier > later);
        assert_eq!(earlier.primary, later.primary);
        assert_eq!(earlier.cardinality, ExactWeight::one());
        assert_eq!(later.cardinality, ExactWeight::one());
    }

    #[test]
    fn exact_weight_matches_independent_scaled_integer_oracle() {
        let weights = [-1.5, -0.25, 0.0, 0.5, 2.0];
        for first in weights {
            for second in weights {
                for third in weights {
                    let values = [first, second, third];
                    let oracle =
                        (first * 4.0) as i128 + (second * 4.0) as i128 + (third * 4.0) as i128;
                    assert_eq!(
                        exact(&values).cmp(&ExactWeight::default()),
                        oracle.cmp(&0),
                        "values={values:?}"
                    );
                    assert_eq!(exact(&values), exact(&[third, first, second]));
                }
            }
        }
    }

    #[test]
    fn initializes_feasible_duals_and_extends_fifo_forest() {
        let edges = [edge(9, 0, 1, 4.0), edge(8, 1, 2, -3.0)];
        let mut state =
            AlternatingDualState::from_seed(4, &edges, &[(1, 2, 1)], &[(0, 1, 0)]).unwrap();
        assert_eq!(state.slack(&indexed(0, 0, 1, 4.0)).unwrap(), value(0.0));
        assert!(state.slack(&indexed(1, 1, 2, -3.0)).unwrap() > value(0.0));
        assert!(state.slack(&indexed(1, 1, 2, -f64::MAX)).unwrap() > value(0.0));
        assert_eq!(
            [state.pop_outer(), state.pop_outer(), state.pop_outer()],
            [Some(0), Some(3), Some(2)]
        );
    }

    #[test]
    fn rejects_asymmetric_or_invalid_transitions() {
        assert!(AlternatingDualState::from_seed(3, &[], &[(0, 0, 0)], &[]).is_err());
        assert!(AlternatingDualState::from_seed(3, &[], &[(1, 2, 0), (0, 1, 1)], &[]).is_err());
        assert!(AlternatingDualState::from_seed(3, &[], &[(1, 2, 0)], &[(0, 0, 1)]).is_err());
        let state = AlternatingDualState::new(3, &[]).unwrap();
        assert!(state.slack(&indexed(0, 0, 9, 1.0)).is_err());
        assert!(state.slack(&indexed(0, 0, 1, f64::NAN)).is_err());
        let mut broken = AlternatingDualState::new(2, &[]).unwrap();
        broken.vertices[0].mate = Some((1, 0));
        assert!(broken.validate().is_err());
        broken.vertices[0].mate = None;
        broken.vertices[0].root = None;
        assert!(broken.validate().is_err());
    }

    #[test]
    fn augments_atomically_and_rebuilds_free_roots() {
        let mut state = AlternatingDualState::from_seed(5, &[], &[(1, 2, 11)], &[]).unwrap();
        state.augment(&[0, 1, 2, 3], &[10, 11, 12]).unwrap();
        assert_eq!(state.vertices[0].mate, Some((1, 10)));
        assert_eq!(state.vertices[1].mate, Some((0, 10)));
        assert_eq!(state.vertices[2].mate, Some((3, 12)));
        assert_eq!(state.vertices[3].mate, Some((2, 12)));
        assert_eq!(state.pop_outer(), Some(4));
        let before = state.vertices.clone();
        assert!(state.augment(&[0, 2], &[13]).is_err());
        assert_eq!(state.vertices, before);
    }

    #[test]
    fn rebuilds_one_root_for_an_exposed_contracted_representative() {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            5,
            &[],
            &[(1, 2, 12), (3, 4, 34)],
            &[(0, 1, 1), (0, 3, 3)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        for vertex in &mut state.vertices {
            vertex.mate = None;
        }
        state.mate_pair(0, 3, 3).unwrap();
        state.mate_pair(2, 4, 24).unwrap();

        state.reset_forest().unwrap();
        assert_eq!(state.blossoms.representative(1).unwrap(), 0);
        assert_eq!(state.vertices[0].label, AlternatingLabel::Outer);
        assert_eq!(state.vertices[0].root, Some(0));
        assert_eq!(state.vertices[1].label, AlternatingLabel::Free);
        assert_eq!(state.vertices[1].root, None);
        assert_eq!(state.outer_queue.iter().copied().collect::<Vec<_>>(), [0]);
        assert!(state.start_root(1).is_err());
        state.validate().unwrap();
    }

    #[test]
    fn verifies_vertex_certificate_and_deterministic_objective() {
        let mut state = AlternatingDualState::from_seed(3, &[], &[(0, 1, 7)], &[]).unwrap();
        let edges = [
            IndexedWeightedEdge {
                edge: 7,
                left: 1,
                right: 0,
                weight: 5.0,
            },
            IndexedWeightedEdge {
                edge: 8,
                left: 1,
                right: 2,
                weight: 2.0,
            },
        ];
        let matched_dual = ExactMatchingValue::from_edge(&edges[0]).unwrap();
        state.vertices[0].dual = matched_dual.clone();
        state.vertices[1].dual = matched_dual;
        state.vertices[2].dual = value(0.0);
        let objective = state.objective(&edges).unwrap();
        assert_eq!(
            objective,
            MatchingObjective {
                weight: exact(&[5.0]),
                cardinality: 1,
                edges: vec![7],
            }
        );
        state
            .check_vertex_optimality(&edges, Some(&objective))
            .unwrap();
        let cardinality = MatchingObjective {
            weight: exact(&[5.0]),
            cardinality: 2,
            edges: vec![8, 9],
        };
        assert!(AlternatingDualState::compare_objectives(&cardinality, &objective).is_gt());
        state.vertices[1].dual = value(4.0);
        assert!(state.check_vertex_optimality(&edges, None).is_err());
    }

    #[test]
    fn retires_a_zero_dual_exposed_tree_without_stopping_other_roots() {
        let mut state = AlternatingDualState::new(2, &[]).unwrap();
        state.vertices[1].dual = value(1.0);

        assert_eq!(
            state
                .solve_exact(&[], &control(AlgorithmCancellation::default()))
                .unwrap(),
            Vec::<usize>::new()
        );
        assert!(
            state
                .vertices
                .iter()
                .all(|vertex| vertex.dual == ExactMatchingValue::default())
        );
        assert!(state.outer_queue.is_empty());
        state.check_vertex_optimality(&[], None).unwrap();
    }

    #[test]
    fn rebuilds_once_when_a_zero_dual_outer_descendant_blocks_the_root() {
        let mut state =
            AlternatingDualState::from_seed(3, &[], &[(0, 1, 0)], &[(2, 0, 1)]).unwrap();
        state.vertices[2].dual = value(1.0);

        let (delta, action) = state
            .dual_step(&[], &control(AlgorithmCancellation::default()), &mut 0)
            .unwrap();

        assert_eq!(delta, ExactMatchingValue::default());
        assert_eq!(action, None);
        assert!(!state.retire_zero_dual_roots().unwrap());
        assert!(state.reset_forest_if_changed().unwrap());
        assert_eq!(state.vertices[0].label, AlternatingLabel::Free);
        assert_eq!(state.vertices[1].label, AlternatingLabel::Free);
        assert_eq!(state.vertices[2].label, AlternatingLabel::Outer);
        assert_eq!(state.outer_queue.iter().copied().collect::<Vec<_>>(), [2]);
        assert!(!state.reset_forest_if_changed().unwrap());
        let (progress, action) = state
            .dual_step(&[], &control(AlgorithmCancellation::default()), &mut 0)
            .unwrap();
        assert!(progress > ExactMatchingValue::default());
        assert_eq!(action, None);
        assert!(state.retire_zero_dual_roots().unwrap());
        state.validate().unwrap();
    }

    #[test]
    fn classifies_tight_edges_in_canonical_identity_order() {
        let mut state = AlternatingDualState::new(3, &[]).unwrap();
        state.vertices[1].label = AlternatingLabel::Free;
        state.vertices[1].root = None;
        let edges = [indexed(9, 0, 1, 2.0), indexed(8, 2, 1, 2.0)];
        state.vertices[0].dual = doubled(ExactMatchingValue::from_edge(&edges[0]).unwrap());
        state.vertices[1].dual = value(0.0);
        state.vertices[2].dual = doubled(ExactMatchingValue::from_edge(&edges[1]).unwrap());
        assert_eq!(
            state
                .tight_edge_action(&edges, &control(AlgorithmCancellation::default()), &mut 0,)
                .unwrap(),
            Some(TightEdgeAction::Grow {
                outer: 2,
                free: 1,
                edge: 8,
            })
        );
        state.vertices[1].label = AlternatingLabel::Outer;
        state.vertices[1].root = Some(0);
        state.vertices[0].root = Some(0);
        assert!(matches!(
            state
                .tight_edge_action(
                    &edges[..1],
                    &control(AlgorithmCancellation::default()),
                    &mut 0,
                )
                .unwrap(),
            Some(TightEdgeAction::Contract { edge: 9, .. })
        ));
        state.vertices[1].root = Some(1);
        assert!(matches!(
            state
                .tight_edge_action(
                    &edges[..1],
                    &control(AlgorithmCancellation::default()),
                    &mut 0,
                )
                .unwrap(),
            Some(TightEdgeAction::Augment { edge: 9, .. })
        ));
    }

    #[test]
    fn excludes_only_the_exact_matched_edge_from_growth() {
        let mut state = AlternatingDualState::from_seed(2, &[], &[(0, 1, 9)], &[]).unwrap();
        state.vertices[0].label = AlternatingLabel::Outer;
        state.vertices[0].root = Some(0);
        state.vertices[1].label = AlternatingLabel::Free;
        state.vertices[1].root = None;
        let matched = indexed(9, 0, 1, 2.0);
        state.vertices[0].dual = doubled(ExactMatchingValue::from_edge(&matched).unwrap());
        state.vertices[1].dual = value(0.0);
        assert_eq!(
            state
                .tight_edge_action(
                    std::slice::from_ref(&matched),
                    &control(AlgorithmCancellation::default()),
                    &mut 0,
                )
                .unwrap(),
            None
        );

        let parallel = indexed(8, 0, 1, 2.0);
        state.vertices[0].dual = doubled(ExactMatchingValue::from_edge(&parallel).unwrap());
        assert_eq!(
            state
                .tight_edge_action(
                    std::slice::from_ref(&parallel),
                    &control(AlgorithmCancellation::default()),
                    &mut 0,
                )
                .unwrap(),
            Some(TightEdgeAction::Grow {
                outer: 0,
                free: 1,
                edge: 8,
            })
        );
    }

    #[test]
    fn applies_minimum_dual_delta_atomically_with_controls() {
        let mut state = AlternatingDualState::new(3, &[]).unwrap();
        let edges = [indexed(0, 0, 2, 3.0), indexed(1, 0, 2, 3.0)];
        let earlier = ExactMatchingValue::from_edge(&edges[0]).unwrap();
        let later = ExactMatchingValue::from_edge(&edges[1]).unwrap();
        assert_eq!(earlier.primary, later.primary);
        assert_eq!(earlier.cardinality, later.cardinality);
        assert!(earlier > later);
        let mut outer_dual = earlier.clone();
        outer_dual += &value(1.0);
        state.vertices[0].dual = outer_dual.clone();
        state.vertices[1].dual = value(0.0);
        state.vertices[2].dual = outer_dual;
        state.vertices[1].label = AlternatingLabel::Free;
        state.vertices[1].root = None;
        state.outer_queue.retain(|&vertex| vertex != 1);
        state.queued[1] = false;
        let (delta, action) = state
            .dual_step(&edges, &control(AlgorithmCancellation::default()), &mut 0)
            .unwrap();
        assert_eq!(delta, value(1.0));
        assert_eq!(state.vertices[0].dual, earlier);
        assert_eq!(state.vertices[2].dual, earlier);
        assert!(matches!(
            action,
            Some(TightEdgeAction::Augment { edge: 0, .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let before = state.vertices.clone();
        assert!(
            state
                .dual_step(&edges, &control(cancellation), &mut 0)
                .is_err()
        );
        assert_eq!(state.vertices, before);
        assert!(
            state
                .dual_step(
                    &[indexed(0, 0, 1, f64::NAN)],
                    &control(AlgorithmCancellation::default()),
                    &mut 0,
                )
                .is_err()
        );
        let mut limits = AlgorithmLimits::default();
        limits.iterations = 0;
        assert!(
            state
                .dual_step(
                    &edges,
                    &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                    &mut 4_095,
                )
                .is_err()
        );
        let mut subnormal = AlternatingDualState::new(2, &[]).unwrap();
        let subnormal_edges = [indexed(0, 0, 1, f64::from_bits(1))];
        let mut half = value(f64::from_bits(1));
        half >>= 1;
        let mut subnormal_dual = ExactMatchingValue::from_edge(&subnormal_edges[0]).unwrap();
        subnormal_dual += &half;
        subnormal.vertices[0].dual = subnormal_dual.clone();
        subnormal.vertices[1].dual = subnormal_dual;
        let (delta, _) = subnormal
            .dual_step(
                &subnormal_edges,
                &control(AlgorithmCancellation::default()),
                &mut 0,
            )
            .unwrap();
        assert!(delta > value(0.0));
        assert!(delta.primary < exact(&[f64::from_bits(1)]));
        assert_eq!(subnormal.slack(&subnormal_edges[0]).unwrap(), value(0.0));
    }

    #[test]
    fn reconstructs_contracts_and_augments_alternating_paths() {
        let cancellation = AlgorithmCancellation::default();
        let control = control(cancellation);
        let mut cycle = AlternatingDualState::from_seed(
            8,
            &[],
            &[(1, 2, 12), (3, 4, 34), (6, 7, 67)],
            &[(0, 1, 1), (0, 3, 3), (5, 6, 56)],
        )
        .unwrap();
        assert_eq!(
            cycle.root_path(2, &control).unwrap(),
            AlternatingPath {
                vertices: vec![0, 1, 2],
                edges: vec![1, 12],
            }
        );
        cycle.contract_tight_cycle(2, 4, 24, &control).unwrap();
        cycle.augment_tight_paths(2, 7, 27, &control).unwrap();
        assert_eq!(cycle.vertices[0].mate, Some((1, 1)));
        assert_eq!(cycle.vertices[2].mate, Some((7, 27)));
        assert_eq!(cycle.vertices[5].mate, Some((6, 56)));

        let mut state = AlternatingDualState::from_seed(
            6,
            &[],
            &[(1, 2, 12), (4, 5, 45)],
            &[(0, 1, 1), (3, 4, 34)],
        )
        .unwrap();
        state.augment_tight_paths(5, 2, 25, &control).unwrap();
        assert_eq!(state.vertices[0].mate, Some((1, 1)));
        assert_eq!(state.vertices[2].mate, Some((5, 25)));
        assert_eq!(state.vertices[3].mate, Some((4, 34)));
    }

    #[test]
    fn crossing_actions_use_representatives_but_preserve_leaf_endpoints() {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            8,
            &[],
            &[(1, 2, 12), (3, 4, 34), (6, 7, 67)],
            &[(0, 1, 1), (0, 3, 3), (5, 6, 56)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        let crossing = [indexed(70, 1, 7, 2.0)];
        let mut crossing_dual = ExactMatchingValue::from_edge(&crossing[0]).unwrap();
        crossing_dual += &value(1.0);
        for vertex in &mut state.vertices {
            vertex.dual = crossing_dual.clone();
        }
        let (delta, action) = state.dual_step(&crossing, &control, &mut 0).unwrap();
        assert_eq!(
            (delta, state.slack(&crossing[0]).unwrap(), action),
            (
                value(1.0),
                value(0.0),
                Some(TightEdgeAction::Augment {
                    left: 1,
                    right: 7,
                    edge: 70,
                })
            )
        );
        assert_eq!(
            state
                .tight_edge_action(&crossing, &control, &mut 0)
                .unwrap(),
            Some(TightEdgeAction::Augment {
                left: 1,
                right: 7,
                edge: 70,
            })
        );
        state.augment_tight_paths(1, 7, 70, &control).unwrap();
        assert_eq!(state.vertices[1].mate, Some((7, 70)));
        assert_eq!(state.vertices[2].mate, Some((4, 24)));
    }

    #[test]
    fn zero_dual_expansion_reconstructs_labels_parents_roots_and_queue() {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            8,
            &[],
            &[(1, 2, 12), (3, 4, 34)],
            &[(0, 1, 1), (0, 3, 3)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        state.mate_pair(0, 6, 6).unwrap();
        let mut outer_labels = vec![AlternatingLabel::Free; 8];
        outer_labels[0] = AlternatingLabel::Outer;
        state
            .blossoms
            .apply_dual_step(&outer_labels, &value(1.0), &control)
            .unwrap();

        state.outer_queue.clear();
        state.queued.fill(false);
        for vertex in &mut state.vertices {
            vertex.label = AlternatingLabel::Free;
            vertex.parent = None;
            vertex.root = None;
            vertex.dual = value(1.0);
        }
        state.vertices[0].label = AlternatingLabel::Inner;
        state.vertices[0].root = Some(7);
        state.vertices[1].label = AlternatingLabel::Inner;
        state.vertices[1].parent = Some((7, 71));
        state.vertices[1].root = Some(7);
        state.vertices[7].label = AlternatingLabel::Outer;
        state.vertices[7].root = Some(7);
        state.enqueue(7);

        state.vertices[1].root = None;
        let vertices_before = state.vertices.clone();
        let queue_before = state.outer_queue.clone();
        assert!(state.dual_step(&[], &control, &mut 0).is_err());
        assert_eq!(state.vertices, vertices_before);
        assert_eq!(state.outer_queue, queue_before);
        assert_eq!(state.blossoms.representative(1).unwrap(), 0);

        state.vertices[1].root = Some(7);
        state.dual_step(&[], &control, &mut 0).unwrap();
        let cycle = [1, 2, 4, 3, 0];
        for (position, &vertex) in cycle.iter().enumerate() {
            assert_eq!(state.blossoms.representative(vertex).unwrap(), vertex);
            assert_eq!(
                state.vertices[vertex].label,
                if position.is_multiple_of(2) {
                    AlternatingLabel::Inner
                } else {
                    AlternatingLabel::Outer
                }
            );
            assert_eq!(state.vertices[vertex].root, Some(7));
        }
        assert_eq!(state.vertices[1].parent, Some((7, 71)));
        assert_eq!(state.vertices[2].parent, Some((1, 12)));
        assert_eq!(state.vertices[4].parent, Some((2, 24)));
        assert_eq!(state.vertices[3].parent, Some((4, 34)));
        assert_eq!(state.vertices[0].parent, Some((3, 3)));
        assert_eq!(state.vertices[6].label, AlternatingLabel::Outer);
        assert_eq!(state.vertices[6].parent, Some((0, 6)));
        assert_eq!(state.vertices[6].root, Some(7));
        assert_eq!(
            state.outer_queue.iter().copied().collect::<Vec<_>>(),
            [7, 6, 2, 3]
        );
        state.validate().unwrap();
    }

    #[test]
    fn nested_expansion_reconstruction_uses_the_outermost_leaf_cycle() {
        let mut state =
            AlternatingDualState::from_seed(8, &[], &[(0, 6, 6), (1, 2, 12), (3, 4, 34)], &[])
                .unwrap();
        state.outer_queue.clear();
        state.queued.fill(false);
        for vertex in &mut state.vertices {
            vertex.label = AlternatingLabel::Free;
            vertex.parent = None;
            vertex.root = None;
        }
        state.vertices[1].label = AlternatingLabel::Inner;
        state.vertices[1].parent = Some((7, 70));
        state.vertices[1].root = Some(7);
        state.vertices[7].label = AlternatingLabel::Outer;
        state.vertices[7].root = Some(7);
        state.enqueue(7);
        state
            .reconstruct_expansions(&[
                (vec![0, 1, 2, 3, 4], vec![10, 12, 23, 34, 40]),
                (vec![0, 1, 2], vec![20, 21, 22]),
            ])
            .unwrap();
        assert_eq!(state.vertices[1].parent, Some((7, 70)));
        assert_eq!(state.vertices[2].parent, Some((1, 12)));
        assert_eq!(state.vertices[3].parent, Some((2, 23)));
        assert_eq!(state.vertices[6].label, AlternatingLabel::Outer);
        assert_eq!(state.vertices[6].parent, Some((0, 6)));
        assert_eq!(state.vertices[6].root, Some(7));
        assert_eq!(
            state.outer_queue.iter().copied().collect::<Vec<_>>(),
            [7, 6, 2, 4]
        );
        state.validate().unwrap();
    }

    #[test]
    fn expansion_uses_parent_direction_for_an_external_mate() {
        let mut state =
            AlternatingDualState::from_seed(5, &[], &[(0, 3, 30), (1, 2, 12)], &[]).unwrap();
        state.vertices[0].parent = Some((4, 40));
        state.vertices[1].parent = Some((0, 10));
        state.vertices[2].parent = Some((1, 21));
        let cycle = [2, 1, 0];
        let edges = [21, 10, 20];

        assert_eq!(
            state.expanded_tree_path(&cycle, &edges).unwrap(),
            (vec![0, 1, 2], vec![10, 21])
        );

        state.vertices[2].parent = Some((0, 20));
        assert!(state.expanded_tree_path(&cycle, &edges).is_err());
    }

    #[test]
    fn dual_bound_includes_every_leaf_of_an_outer_blossom() {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            5,
            &[],
            &[(1, 2, 12), (3, 4, 34)],
            &[(0, 1, 1), (0, 3, 3)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        for vertex in &mut state.vertices {
            vertex.dual = value(3.0);
        }
        state.vertices[1].dual = value(0.5);
        let (delta, _) = state.dual_step(&[], &control, &mut 0).unwrap();
        assert_eq!(delta, value(0.5));
        assert_eq!(state.vertices[1].dual, value(0.0));
    }

    #[test]
    fn grow_lifts_through_an_unlabeled_contracted_blossom() {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            10,
            &[],
            &[(1, 2, 12), (3, 4, 34), (6, 7, 67)],
            &[(0, 1, 1), (0, 3, 3), (5, 6, 56)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        state.mate_pair(0, 8, 80).unwrap();
        for vertex in [0, 8] {
            state.vertices[vertex].label = AlternatingLabel::Free;
            state.vertices[vertex].parent = None;
            state.vertices[vertex].root = None;
        }
        assert_eq!(state.vertices[1].label, AlternatingLabel::Inner);

        assert_eq!(state.extend(9, 1, 91).unwrap(), 8);
        assert_eq!(state.vertices[0].label, AlternatingLabel::Inner);
        assert_eq!(state.vertices[8].label, AlternatingLabel::Outer);
        let path = state.root_path(8, &control).unwrap();
        assert_eq!(path.vertices.first(), Some(&9));
        assert_eq!(path.vertices.last(), Some(&8));
        assert_eq!(path.edges.first(), Some(&91));
        assert_eq!(path.edges.last(), Some(&80));
    }

    fn state_with_a_nonrepresentative_crossing_mate() -> AlternatingDualState {
        let control = control(AlgorithmCancellation::default());
        let mut state = AlternatingDualState::from_seed(
            10,
            &[],
            &[(1, 2, 12), (3, 4, 34), (6, 7, 67)],
            &[(0, 1, 1), (0, 3, 3), (5, 6, 56)],
        )
        .unwrap();
        state.contract_tight_cycle(2, 4, 24, &control).unwrap();
        for vertex in 0..=4 {
            state.vertices[vertex].mate = None;
            state.vertices[vertex].label = AlternatingLabel::Free;
            state.vertices[vertex].parent = None;
            state.vertices[vertex].root = None;
        }
        state.vertices[8].label = AlternatingLabel::Free;
        state.vertices[8].root = None;
        state.mate_pair(0, 3, 3).unwrap();
        state.mate_pair(2, 4, 24).unwrap();
        state.mate_pair(1, 8, 80).unwrap();
        state
    }

    #[test]
    fn grow_uses_a_nonrepresentative_blossom_crossing_mate() {
        let control = control(AlgorithmCancellation::default());
        let mut state = state_with_a_nonrepresentative_crossing_mate();
        assert_eq!(state.blossoms.representative(1).unwrap(), 0);
        assert_eq!(state.extend(9, 1, 91).unwrap(), 8);
        assert_eq!(state.vertices[8].parent, Some((1, 80)));
        assert_eq!(
            state.root_path(8, &control).unwrap(),
            AlternatingPath {
                vertices: vec![9, 1, 8],
                edges: vec![91, 80],
            }
        );
    }

    #[test]
    fn rejects_multiple_blossom_crossing_mates() {
        let mut state = state_with_a_nonrepresentative_crossing_mate();
        state.vertices[0].mate = None;
        state.vertices[3].mate = None;
        state.mate_pair(0, 9, 90).unwrap();
        assert!(matches!(
            state.crossing_mate(0),
            Err(AlgorithmError::Execution { message })
                if message == "alternating blossom has multiple crossing mates"
        ));
    }

    #[test]
    fn tight_edge_scan_accounts_for_work_and_observes_cancellation() {
        let state = AlternatingDualState::new(2, &[]).unwrap();
        let edges = vec![indexed(0, 0, 1, 0.0); 4_096];
        let mut limits = AlgorithmLimits::default();
        limits.iterations = 0;
        let error = state
            .tight_edge_action(
                &edges,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                &mut 0,
            )
            .unwrap_err();
        assert!(matches!(error, AlgorithmError::IterationLimit { .. }));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            state.tight_edge_action(&edges, &control(cancellation), &mut 0),
            Err(AlgorithmError::Cancelled)
        ));
    }

    #[test]
    fn path_reconstruction_controls_fail_without_matching_mutation() {
        let mut state =
            AlternatingDualState::from_seed(3, &[], &[(1, 2, 12)], &[(0, 1, 1)]).unwrap();
        let before = state.vertices.clone();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(
            state
                .augment_tight_paths(0, 2, 2, &control(cancellation))
                .is_err()
        );
        assert_eq!(state.vertices, before);
        assert!(
            state
                .contract_tight_cycle(
                    0,
                    2,
                    2,
                    &AlgorithmControl::new(
                        AlgorithmLimits {
                            iterations: 0,
                            ..AlgorithmLimits::default()
                        },
                        AlgorithmCancellation::default(),
                    ),
                )
                .is_err()
        );
        assert_eq!(state.vertices, before);
    }

    #[test]
    fn solver_loop_returns_only_after_an_exact_certificate() {
        let weighted = [
            edge(0, 0, 1, 4.0),
            edge(1, 1, 2, 5.0),
            edge(2, 2, 3, 4.0),
            edge(3, 3, 0, 5.0),
            edge(4, 0, 2, 6.0),
        ];
        let edges = weighted
            .iter()
            .enumerate()
            .map(|(edge, value)| {
                indexed(
                    edge,
                    value.source_uuid[0] as usize,
                    value.target_uuid[0] as usize,
                    value.weight,
                )
            })
            .collect::<Vec<_>>();
        let mut state = AlternatingDualState::new(4, &weighted).unwrap();

        let selected = state
            .solve_exact(&edges, &control(AlgorithmCancellation::default()))
            .unwrap();

        assert_eq!(selected, vec![1, 3]);
        state.check_vertex_optimality(&edges, None).unwrap();
    }

    #[test]
    fn solver_preserves_exposed_duals_through_topology_59() {
        let weighted = [
            edge(0, 0, 1, 1.0),
            edge(1, 0, 2, 1.0),
            edge(2, 0, 4, 1.0),
            edge(3, 1, 2, 1.0),
            edge(4, 1, 3, 1.0),
        ];
        let edges = [
            indexed(0, 0, 1, 1.0),
            indexed(1, 0, 2, 1.0),
            indexed(2, 0, 4, 1.0),
            indexed(3, 1, 2, 1.0),
            indexed(4, 1, 3, 1.0),
        ];
        let mut state = AlternatingDualState::new(5, &weighted).unwrap();

        let selected = state
            .solve_exact(&edges, &control(AlgorithmCancellation::default()))
            .unwrap();

        assert_eq!(selected, vec![1, 4]);
        state.check_vertex_optimality(&edges, None).unwrap();
    }

    #[test]
    fn solver_disambiguates_topology_119_expansion_by_matching_parity() {
        let weighted = [
            edge(0, 0, 1, 1.0),
            edge(1, 0, 2, 1.0),
            edge(2, 0, 3, 1.0),
            edge(3, 1, 2, 1.0),
            edge(4, 1, 3, 1.0),
            edge(5, 1, 4, 1.0),
        ];
        let edges = [
            indexed(0, 0, 1, 1.0),
            indexed(1, 0, 2, 1.0),
            indexed(2, 0, 3, 1.0),
            indexed(3, 1, 2, 1.0),
            indexed(4, 1, 3, 1.0),
            indexed(5, 1, 4, 1.0),
        ];
        let mut state = AlternatingDualState::new(5, &weighted).unwrap();

        let selected = state
            .solve_exact(&edges, &control(AlgorithmCancellation::default()))
            .unwrap();

        assert_eq!(selected, vec![1, 4]);
        state.check_vertex_optimality(&edges, None).unwrap();
    }

    #[test]
    fn solver_reaches_an_exact_certificate_for_topology_123() {
        let weighted = [
            edge(0, 0, 1, 1.0),
            edge(1, 0, 2, 1.0),
            edge(2, 0, 4, 1.0),
            edge(3, 1, 2, 1.0),
            edge(4, 1, 3, 1.0),
            edge(5, 1, 4, 1.0),
        ];
        let edges = [
            indexed(0, 0, 1, 1.0),
            indexed(1, 0, 2, 1.0),
            indexed(2, 0, 4, 1.0),
            indexed(3, 1, 2, 1.0),
            indexed(4, 1, 3, 1.0),
            indexed(5, 1, 4, 1.0),
        ];
        let mut state = AlternatingDualState::new(5, &weighted).unwrap();

        let selected = state
            .solve_exact(&edges, &control(AlgorithmCancellation::default()))
            .unwrap();

        assert_eq!(selected, vec![1, 4]);
        state.check_vertex_optimality(&edges, None).unwrap();
    }

    #[test]
    fn solver_reaches_an_exact_certificate_for_topology_221() {
        let weighted = [
            edge(0, 0, 1, 1.0),
            edge(1, 0, 3, 1.0),
            edge(2, 0, 4, 1.0),
            edge(3, 1, 2, 1.0),
            edge(4, 1, 4, 1.0),
            edge(5, 2, 3, 1.0),
        ];
        let edges = [
            indexed(0, 0, 1, 1.0),
            indexed(1, 0, 3, 1.0),
            indexed(2, 0, 4, 1.0),
            indexed(3, 1, 2, 1.0),
            indexed(4, 1, 4, 1.0),
            indexed(5, 2, 3, 1.0),
        ];
        let mut state = AlternatingDualState::new(5, &weighted).unwrap();

        let selected = state
            .solve_exact(&edges, &control(AlgorithmCancellation::default()))
            .unwrap();

        assert_eq!(selected, vec![0, 5]);
        state.check_vertex_optimality(&edges, None).unwrap();
    }

    #[test]
    fn solver_loop_failure_does_not_publish_partial_state() {
        let weighted = [edge(0, 0, 1, 1.0), edge(1, 1, 2, 2.0)];
        let edges = [indexed(0, 0, 1, 1.0), indexed(1, 1, 2, 2.0)];
        let mut state = AlternatingDualState::new(3, &weighted).unwrap();
        let before = state.objective(&edges).unwrap();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            state.solve_exact(&edges, &control(cancellation)),
            Err(AlgorithmError::Cancelled)
        ));
        assert_eq!(state.objective(&edges).unwrap(), before);

        let control = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );

        assert!(matches!(
            state.solve_exact(&edges, &control),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert_eq!(state.objective(&edges).unwrap(), before);
    }
}
