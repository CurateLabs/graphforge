//! Per-ordinal node label index.
//!
//! A destination label test such as `(:Post)` lowers to
//! `array_has(type_ids, id)`, which demands the `type_ids` column of every node
//! a hop reaches and so forces a node-file read for facts the topology already
//! determines. This index holds those facts in memory: one slot per node
//! ordinal (`node_id`) naming an entry of a small dictionary of label sets.
//! Each entry keeps the node's immutable primary `type_id` and its full
//! authoritative `type_ids` list, in file order, so a lookup returns exactly the
//! values the node file holds.
//!
//! The index is built only from the node files an authenticated inventory
//! admits. It describes one topology generation; its owner discards it when the
//! generation changes. An ordinal no admitted file holds (a deleted node, or a
//! gap the deletion left) stays at the absent sentinel and is omitted from a
//! gather, so the caller reports the same unknown-node error a file read does.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ListArray, UInt32Array, UInt64Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use graphforge_core::GfError;
use graphforge_value::{EntityTypeId, PrimaryEntityTypeId};

use crate::{TopologyFiles, io_stats};

/// Sentinel slot for an ordinal that no admitted node file holds.
const NARROW_ABSENT: u8 = u8::MAX;
const WIDE_ABSENT: u32 = u32::MAX;
/// Dictionary entries addressable by a one-byte slot (the sentinel is reserved).
const NARROW_CAPACITY: usize = NARROW_ABSENT as usize;

/// One distinct label set of a node: the immutable primary routing scalar and
/// the full current membership list, both exactly as the node file stores them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLabelSet {
    primary: u32,
    type_ids: Box<[u32]>,
}

impl NodeLabelSet {
    /// The persisted `type_id` scalar (`u32::MAX` for an originally unlabelled node).
    #[must_use]
    pub fn primary(&self) -> u32 {
        self.primary
    }

    /// The persisted `type_ids` membership list.
    #[must_use]
    pub fn type_ids(&self) -> &[u32] {
        &self.type_ids
    }

    fn heap_bytes(&self) -> u64 {
        (size_of::<Self>() + self.type_ids.len() * size_of::<u32>()) as u64
    }
}

#[derive(Debug)]
enum Slots {
    Narrow(Vec<u8>),
    Wide(Vec<u32>),
}

impl Slots {
    fn width(&self) -> usize {
        match self {
            Self::Narrow(_) => size_of::<u8>(),
            Self::Wide(_) => size_of::<u32>(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Narrow(slots) => slots.len(),
            Self::Wide(slots) => slots.len(),
        }
    }

    fn get(&self, ordinal: usize) -> Option<usize> {
        match self {
            Self::Narrow(slots) => slots
                .get(ordinal)
                .copied()
                .filter(|slot| *slot != NARROW_ABSENT)
                .map(usize::from),
            Self::Wide(slots) => slots
                .get(ordinal)
                .copied()
                .filter(|slot| *slot != WIDE_ABSENT)
                .map(|slot| slot as usize),
        }
    }

    /// Store `set` at `ordinal`; return false when the ordinal was already
    /// occupied (a duplicate node identity).
    fn assign(&mut self, ordinal: usize, set: usize) -> bool {
        match self {
            Self::Narrow(slots) => {
                if slots[ordinal] != NARROW_ABSENT {
                    return false;
                }
                slots[ordinal] = u8::try_from(set).expect("narrow slot width checked by caller");
            }
            Self::Wide(slots) => {
                if slots[ordinal] != WIDE_ABSENT {
                    return false;
                }
                slots[ordinal] = u32::try_from(set).expect("dictionary size checked by caller");
            }
        }
        true
    }

    fn grow_to(&mut self, len: usize) {
        match self {
            Self::Narrow(slots) if slots.len() < len => slots.resize(len, NARROW_ABSENT),
            Self::Wide(slots) if slots.len() < len => slots.resize(len, WIDE_ABSENT),
            _ => {}
        }
    }

    fn widen(&mut self) {
        if let Self::Narrow(slots) = self {
            let wide = slots
                .iter()
                .map(|slot| {
                    if *slot == NARROW_ABSENT {
                        WIDE_ABSENT
                    } else {
                        u32::from(*slot)
                    }
                })
                .collect();
            *self = Self::Wide(wide);
        }
    }
}

/// Node label sets by ordinal for one topology generation.
#[derive(Debug)]
pub struct NodeLabelIndex {
    slots: Slots,
    sets: Vec<NodeLabelSet>,
}

/// Columns gathered for the nodes a chunk reached. Only requested columns are
/// built; `node_ids` always lists the ordinals the index holds, in input order.
#[derive(Debug)]
pub struct GatheredNodeLabels {
    /// Ordinals present in the index. An ordinal the index lacks is omitted.
    pub node_ids: UInt64Array,
    /// Persisted `type_id` scalars, parallel to `node_ids`.
    pub type_id: Option<UInt32Array>,
    /// Persisted `type_ids` lists, parallel to `node_ids`; the item field
    /// matches the topology node schema.
    pub type_ids: Option<ListArray>,
}

impl NodeLabelIndex {
    /// Build the index from the node files an authenticated inventory admits.
    ///
    /// Returns `Ok(None)` when the index would exceed `budget_bytes`; the caller
    /// then reads node files as before. Any unreadable or invalid file, and any
    /// node identity held twice, is an error.
    ///
    /// # Errors
    /// Fails closed on a node file the storage admission policy rejects, a
    /// missing or mistyped label column, an invalid label token, or a duplicate
    /// `node_id`.
    pub fn build(files: &TopologyFiles, budget_bytes: u64) -> Result<Option<Self>, GfError> {
        let mut builder = Builder::new(budget_bytes);
        for (path, _) in &files.nodes {
            let batches = crate::catalog::read_node_label_columns(path).map_err(|error| {
                GfError::Storage(format!("node label index: {}: {error}", path.display()))
            })?;
            for batch in &batches {
                if !builder.absorb(batch)? {
                    return Ok(None);
                }
            }
        }
        let index = builder.finish();
        io_stats::record_node_label_index_build(index.bytes());
        Ok(Some(index))
    }

    /// The label set of `ordinal`, or `None` for an ordinal no admitted file holds.
    #[must_use]
    pub fn get(&self, ordinal: u64) -> Option<&NodeLabelSet> {
        let ordinal = usize::try_from(ordinal).ok()?;
        self.slots.get(ordinal).map(|set| &self.sets[set])
    }

    /// Resident size: one slot per ordinal plus the label-set dictionary.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        (self.slots.len() * self.slots.width()) as u64
            + self.sets.iter().map(NodeLabelSet::heap_bytes).sum::<u64>()
    }

    /// Number of distinct label sets.
    #[must_use]
    pub fn distinct_sets(&self) -> usize {
        self.sets.len()
    }

    /// Gather labels for one chunk of reached ordinals and record the lookup.
    ///
    /// Ordinals the index lacks are omitted rather than defaulted, so a caller
    /// that asked for a node nobody holds still observes it as missing.
    ///
    /// # Errors
    /// Fails if the gathered membership lists exceed Arrow's list offset range.
    pub fn gather(
        &self,
        ordinals: &[u64],
        want_type_id: bool,
        want_type_ids: bool,
    ) -> Result<GatheredNodeLabels, GfError> {
        io_stats::record_node_label_index_lookup();
        let mut node_ids = Vec::with_capacity(ordinals.len());
        let mut primaries = Vec::with_capacity(if want_type_id { ordinals.len() } else { 0 });
        let mut lengths = Vec::with_capacity(if want_type_ids { ordinals.len() } else { 0 });
        let mut members: Vec<u32> = Vec::new();
        for &ordinal in ordinals {
            let Some(set) = self.get(ordinal) else {
                continue;
            };
            node_ids.push(ordinal);
            if want_type_id {
                primaries.push(set.primary);
            }
            if want_type_ids {
                lengths.push(set.type_ids.len());
                members.extend_from_slice(&set.type_ids);
            }
        }
        let type_ids = if want_type_ids {
            if i32::try_from(members.len()).is_err() {
                return Err(GfError::Execution(
                    "node label index: gathered memberships exceed list offsets".into(),
                ));
            }
            Some(ListArray::new(
                Arc::new(Field::new("item", DataType::UInt32, false)),
                OffsetBuffer::from_lengths(lengths),
                Arc::new(UInt32Array::from(members)),
                None,
            ))
        } else {
            None
        };
        Ok(GatheredNodeLabels {
            node_ids: UInt64Array::from(node_ids),
            type_id: want_type_id.then(|| UInt32Array::from(primaries)),
            type_ids,
        })
    }
}

struct Builder {
    slots: Slots,
    sets: Vec<NodeLabelSet>,
    by_key: HashMap<Box<[u32]>, usize>,
    budget_bytes: u64,
    dictionary_bytes: u64,
    scratch: Vec<u32>,
    last_set: Option<usize>,
}

impl Builder {
    fn new(budget_bytes: u64) -> Self {
        Self {
            slots: Slots::Narrow(Vec::new()),
            sets: Vec::new(),
            by_key: HashMap::new(),
            budget_bytes,
            dictionary_bytes: 0,
            scratch: Vec::new(),
            last_set: None,
        }
    }

    fn finish(self) -> NodeLabelIndex {
        NodeLabelIndex {
            slots: self.slots,
            sets: self.sets,
        }
    }

    fn over_budget(&self, slots: usize, width: usize) -> bool {
        (slots as u64)
            .saturating_mul(width as u64)
            .saturating_add(self.dictionary_bytes)
            > self.budget_bytes
    }

    /// Fold one projected batch in. Returns false when the index outgrows its budget.
    fn absorb(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<bool, GfError> {
        let invalid = |message: &str| GfError::Storage(format!("node label index: {message}"));
        if batch.num_rows() == 0 {
            return Ok(true);
        }
        let node_ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid("node_id is not UInt64"))?;
        let primaries = batch
            .column_by_name("type_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| invalid("type_id is not UInt32"))?;
        let memberships = batch
            .column_by_name("type_ids")
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| invalid("type_ids is not List<UInt32>"))
            })
            .transpose()?;
        if node_ids.null_count() != 0 || primaries.null_count() != 0 {
            return Err(invalid("null node_id or type_id"));
        }
        let member_values = match memberships {
            Some(lists) => {
                if lists.null_count() != 0 {
                    return Err(invalid("null membership list"));
                }
                let values = lists
                    .values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("type_ids values are not UInt32"))?;
                if values.null_count() != 0 {
                    return Err(invalid("null membership"));
                }
                Some(values)
            }
            None => None,
        };

        let highest = node_ids.values().iter().copied().max().unwrap_or(0);
        let needed = usize::try_from(highest)
            .ok()
            .and_then(|highest| highest.checked_add(1));
        let Some(needed) = needed else {
            return Ok(false);
        };
        if needed > self.slots.len() {
            if self.over_budget(needed, self.slots.width()) {
                return Ok(false);
            }
            self.slots.grow_to(needed);
        }

        for row in 0..batch.num_rows() {
            let primary = primaries.value(row);
            self.scratch.clear();
            self.scratch.push(primary);
            if let (Some(lists), Some(values)) = (memberships, member_values) {
                let offsets = lists.value_offsets();
                let start = usize::try_from(offsets[row])
                    .map_err(|_| invalid("negative membership offset"))?;
                let end = usize::try_from(offsets[row + 1])
                    .map_err(|_| invalid("negative membership offset"))?;
                self.scratch.extend_from_slice(&values.values()[start..end]);
            } else {
                // A file written before multi-label membership: the primary
                // is the singleton membership, and an unlabelled node has none.
                let primary = PrimaryEntityTypeId::decode(primary)
                    .map_err(|error| invalid(&format!("primary row {row}: {error}")))?;
                if let Some(label) = primary.label() {
                    self.scratch.push(label.encode());
                }
            }
            let set = match self.last_set {
                Some(last) if self.sets[last].matches(&self.scratch) => last,
                _ => match self.by_key.get(self.scratch.as_slice()) {
                    Some(&set) => set,
                    None => match self.insert_set()? {
                        Some(set) => set,
                        None => return Ok(false),
                    },
                },
            };
            self.last_set = Some(set);
            let ordinal = usize::try_from(node_ids.value(row))
                .map_err(|_| invalid("node_id exceeds the address space"))?;
            if !self.slots.assign(ordinal, set) {
                return Err(invalid("a node_id appears in more than one node row"));
            }
        }
        Ok(true)
    }

    /// Validate and register the label set in `scratch`. `None` means the
    /// widened index would exceed the budget.
    fn insert_set(&mut self) -> Result<Option<usize>, GfError> {
        let invalid = |message: String| GfError::Storage(format!("node label index: {message}"));
        PrimaryEntityTypeId::decode(self.scratch[0])
            .map_err(|error| invalid(format!("invalid primary type_id: {error}")))?;
        for raw in &self.scratch[1..] {
            EntityTypeId::decode(*raw)
                .map_err(|error| invalid(format!("invalid membership token: {error}")))?;
        }
        let set = self.sets.len();
        if u32::try_from(set + 1).is_err() {
            return Err(invalid(
                "more distinct label sets than slots can address".into(),
            ));
        }
        if matches!(self.slots, Slots::Narrow(_)) && set >= NARROW_CAPACITY {
            if self.over_budget(self.slots.len(), size_of::<u32>()) {
                return Ok(None);
            }
            self.slots.widen();
        }
        let entry = NodeLabelSet {
            primary: self.scratch[0],
            type_ids: self.scratch[1..].into(),
        };
        self.dictionary_bytes += entry.heap_bytes();
        self.by_key.insert(self.scratch.as_slice().into(), set);
        self.sets.push(entry);
        if self.over_budget(self.slots.len(), self.slots.width()) {
            return Ok(None);
        }
        Ok(Some(set))
    }
}

impl NodeLabelSet {
    fn matches(&self, key: &[u32]) -> bool {
        key.split_first()
            .is_some_and(|(primary, ids)| *primary == self.primary && ids == &*self.type_ids)
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use arrow::array::{ArrayRef, ListBuilder, UInt32Builder};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    const UNLABELLED: u32 = u32::MAX;

    fn node_file(
        dir: &Path,
        name: &str,
        rows: &[(u64, u32, Option<&[u32]>)],
        with_type_ids: bool,
    ) -> (PathBuf, String) {
        let mut fields = vec![
            Field::new("node_id", DataType::UInt64, false),
            Field::new("type_id", DataType::UInt32, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ];
        if with_type_ids {
            let mut lists = ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
                "item",
                DataType::UInt32,
                false,
            ));
            for (_, _, members) in rows {
                for member in members.unwrap_or(&[]) {
                    lists.values().append_value(*member);
                }
                lists.append(true);
            }
            let lists = lists.finish();
            fields.push(Field::new("type_ids", lists.data_type().clone(), false));
            columns.push(Arc::new(lists));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).expect("batch");
        let path = dir.join(name);
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        (path, name.to_owned())
    }

    fn files(nodes: Vec<(PathBuf, String)>) -> TopologyFiles {
        TopologyFiles {
            nodes,
            edges: Vec::new(),
        }
    }

    #[test]
    fn preserves_file_order_and_primary_independent_of_membership() {
        let dir = tempfile::tempdir().expect("dir");
        // The primary is immutable routing authority and need not be a member.
        let rows: &[(u64, u32, Option<&[u32]>)] = &[
            (0, 4, Some(&[9, 4])),
            (1, UNLABELLED, Some(&[])),
            (2, 4, Some(&[4, 9])),
            (3, 4, Some(&[9, 4])),
        ];
        let node_files = vec![node_file(dir.path(), "a.parquet", rows, true)];
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        assert_eq!(index.distinct_sets(), 3);
        assert_eq!(index.get(0).expect("0").type_ids(), &[9, 4]);
        assert_eq!(index.get(0).expect("0").primary(), 4);
        assert_eq!(index.get(1).expect("1").primary(), UNLABELLED);
        assert!(index.get(1).expect("1").type_ids().is_empty());
        assert_eq!(index.get(2).expect("2").type_ids(), &[4, 9]);
        assert_eq!(index.get(3), index.get(0));
    }

    #[test]
    fn absent_ordinals_are_omitted_from_a_gather() {
        let dir = tempfile::tempdir().expect("dir");
        // Ordinal 1 was deleted; 5 lies beyond every file.
        let rows: &[(u64, u32, Option<&[u32]>)] = &[
            (0, 4, Some(&[4])),
            (2, 7, Some(&[7, 4])),
            (3, 4, Some(&[4])),
        ];
        let node_files = vec![node_file(dir.path(), "a.parquet", rows, true)];
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        assert!(index.get(1).is_none());
        assert!(index.get(5).is_none());
        assert!(index.get(u64::MAX).is_none());
        let gathered = index.gather(&[3, 1, 2, 5, 0], true, true).expect("gather");
        assert_eq!(gathered.node_ids.values().to_vec(), vec![3, 2, 0]);
        assert_eq!(
            gathered.type_id.expect("type_id").values().to_vec(),
            vec![4, 7, 4]
        );
        let lists = gathered.type_ids.expect("type_ids");
        assert_eq!(lists.value_offsets(), &[0, 1, 3, 4]);
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("u32");
        assert_eq!(values.values().to_vec(), vec![4, 7, 4, 4]);
        assert_eq!(
            lists.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::UInt32, false)))
        );
    }

    #[test]
    fn gather_builds_only_requested_columns() {
        let dir = tempfile::tempdir().expect("dir");
        let rows: &[(u64, u32, Option<&[u32]>)] = &[(0, 4, Some(&[4]))];
        let node_files = vec![node_file(dir.path(), "a.parquet", rows, true)];
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        let only_ids = index.gather(&[0], false, true).expect("gather");
        assert!(only_ids.type_id.is_none() && only_ids.type_ids.is_some());
        let only_primary = index.gather(&[0], true, false).expect("gather");
        assert!(only_primary.type_id.is_some() && only_primary.type_ids.is_none());
    }

    #[test]
    fn a_file_without_memberships_yields_singletons_and_empty_for_unlabelled() {
        let dir = tempfile::tempdir().expect("dir");
        let rows: &[(u64, u32, Option<&[u32]>)] = &[(0, 4, None), (1, UNLABELLED, None)];
        let node_files = vec![node_file(dir.path(), "legacy.parquet", rows, false)];
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        assert_eq!(index.get(0).expect("0").type_ids(), &[4]);
        assert!(index.get(1).expect("1").type_ids().is_empty());
    }

    #[test]
    fn slots_widen_past_the_narrow_dictionary_and_keep_every_assignment() {
        let dir = tempfile::tempdir().expect("dir");
        let distinct = 300_u32;
        let rows: Vec<(u64, u32, Vec<u32>)> = (0..distinct * 2)
            .map(|node| {
                let label = node % distinct;
                (u64::from(node) * 2, label, vec![label])
            })
            .collect();
        let rows: Vec<(u64, u32, Option<&[u32]>)> = rows
            .iter()
            .map(|(node, primary, members)| (*node, *primary, Some(members.as_slice())))
            .collect();
        let node_files = vec![node_file(dir.path(), "wide.parquet", &rows, true)];
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        assert_eq!(index.distinct_sets(), distinct as usize);
        for node in 0..distinct * 2 {
            let label = node % distinct;
            let set = index.get(u64::from(node) * 2).expect("present");
            assert_eq!((set.primary(), set.type_ids()), (label, &[label][..]));
            assert!(index.get(u64::from(node) * 2 + 1).is_none());
        }
    }

    #[test]
    fn exactly_the_narrow_capacity_stays_narrow_and_one_more_widens() {
        let dir = tempfile::tempdir().expect("dir");
        for (distinct, name) in [
            (NARROW_CAPACITY, "edge.parquet"),
            (NARROW_CAPACITY + 1, "over.parquet"),
        ] {
            let rows: Vec<(u64, u32, Vec<u32>)> = (0..distinct as u32)
                .map(|node| (u64::from(node), node, vec![node]))
                .collect();
            let rows: Vec<(u64, u32, Option<&[u32]>)> = rows
                .iter()
                .map(|(node, primary, members)| (*node, *primary, Some(members.as_slice())))
                .collect();
            let node_files = vec![node_file(dir.path(), name, &rows, true)];
            let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
                .expect("build")
                .expect("within budget");
            assert_eq!(
                matches!(index.slots, Slots::Narrow(_)),
                distinct == NARROW_CAPACITY
            );
            for node in 0..distinct as u32 {
                assert_eq!(index.get(u64::from(node)).expect("present").primary(), node);
            }
        }
    }

    #[test]
    fn a_duplicate_node_id_across_files_is_an_error() {
        let dir = tempfile::tempdir().expect("dir");
        let rows: &[(u64, u32, Option<&[u32]>)] = &[(0, 4, Some(&[4])), (1, 4, Some(&[4]))];
        let node_files = vec![
            node_file(dir.path(), "a.parquet", rows, true),
            node_file(dir.path(), "b.parquet", &[(1, 4, Some(&[4]))], true),
        ];
        let error = NodeLabelIndex::build(&files(node_files), u64::MAX).expect_err("duplicate");
        assert!(
            error.to_string().contains("more than one node row"),
            "{error}"
        );
    }

    #[test]
    fn an_invalid_label_token_is_an_error() {
        let dir = tempfile::tempdir().expect("dir");
        // The unlabelled sentinel is a primary scalar, never a membership token.
        let rows: &[(u64, u32, Option<&[u32]>)] = &[(0, 4, Some(&[u32::MAX]))];
        let node_files = vec![node_file(dir.path(), "a.parquet", rows, true)];
        assert!(
            EntityTypeId::decode(u32::MAX).is_err(),
            "fixture assumption"
        );
        let error = NodeLabelIndex::build(&files(node_files), u64::MAX).expect_err("invalid");
        assert!(
            error.to_string().contains("invalid membership token"),
            "{error}"
        );
    }

    #[test]
    fn an_index_larger_than_its_budget_is_unavailable_not_truncated() {
        let dir = tempfile::tempdir().expect("dir");
        let rows: Vec<(u64, u32, Option<&[u32]>)> = (0..1000)
            .map(|node| (node, 4, Some(&[4_u32][..])))
            .collect();
        let node_files = vec![node_file(dir.path(), "a.parquet", &rows, true)];
        let topology = files(node_files);
        assert!(
            NodeLabelIndex::build(&topology, 999)
                .expect("build")
                .is_none()
        );
        let index = NodeLabelIndex::build(&topology, u64::MAX)
            .expect("build")
            .expect("within budget");
        assert!(
            NodeLabelIndex::build(&topology, index.bytes())
                .expect("build")
                .is_some()
        );
        assert!(
            NodeLabelIndex::build(&topology, index.bytes() - 1)
                .expect("build")
                .is_none()
        );
    }

    #[test]
    fn building_counts_one_build_and_no_node_reads_and_a_gather_one_lookup() {
        let dir = tempfile::tempdir().expect("dir");
        let rows: &[(u64, u32, Option<&[u32]>)] = &[(0, 4, Some(&[4]))];
        let node_files = vec![node_file(dir.path(), "a.parquet", rows, true)];
        let _scope = io_stats::CaptureScope::install();
        io_stats::reset();
        let index = NodeLabelIndex::build(&files(node_files), u64::MAX)
            .expect("build")
            .expect("within budget");
        index.gather(&[0], true, true).expect("gather");
        index.gather(&[0], true, true).expect("gather");
        let stats = io_stats::snapshot().expect("requested I/O statistics");
        assert_eq!(stats.node_label_index_builds, 1);
        assert_eq!(stats.node_label_index_bytes, index.bytes());
        assert_eq!(stats.node_label_index_lookups, 2);
        assert_eq!(stats.node_full_reads, 0);
        assert_eq!(stats.node_filtered_reads, 0);
    }
}
