//! Statistics-pruned selection for property reads (#1931).
//!
//! A fragment's Parquet footer carries per-row-group statistics for the UUID
//! key and for every primitive property column. They prune two things without
//! touching a data page: row groups that cannot hold a requested UUID, and
//! fragments or row groups that cannot hold a requested property value.
//!
//! Pruning only ever removes work that cannot change the answer. A missing,
//! deprecated or non-comparable statistic keeps its row group.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use graphforge_ir::IrLiteral;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

use super::{GfError, PropertyFragmentId, corrupt};

/// A property value an equality predicate can compare without coercion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum EqualityValue {
    /// A 64-bit integer.
    Int(i64),
    /// A string.
    Str(String),
    /// A boolean.
    Bool(bool),
}

impl EqualityValue {
    /// The stored Arrow type this value is compared against.
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Int(_) => DataType::Int64,
            Self::Str(_) => DataType::Utf8,
            Self::Bool(_) => DataType::Boolean,
        }
    }

    /// Whether a decoded property value equals this value.
    pub(crate) fn matches(&self, literal: &IrLiteral) -> bool {
        match (self, literal) {
            (Self::Int(expected), IrLiteral::Int(actual)) => expected == actual,
            (Self::Str(expected), IrLiteral::Str(actual)) => expected == actual,
            (Self::Bool(expected), IrLiteral::Bool(actual)) => expected == actual,
            _ => false,
        }
    }
}

/// `column = value` on one stored property column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PropertyEquality {
    /// Property column name.
    pub(crate) column: String,
    /// Value the column must equal.
    pub(crate) value: EqualityValue,
}

impl PropertyEquality {
    /// Whether a snapshot's properties satisfy the predicate. An absent
    /// property is NULL and equals nothing.
    pub(crate) fn holds(&self, values: &std::collections::BTreeMap<String, IrLiteral>) -> bool {
        values
            .get(&self.column)
            .is_some_and(|literal| self.value.matches(literal))
    }
}

/// A property value a range predicate orders without coercion. Integers and
/// UTC microsecond timestamps are both stored as `INT64`, so they share a
/// variant; the scan accepts a bound only against a column of the same type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RangeValue {
    /// A 64-bit integer, or a timestamp in microseconds since the epoch.
    Int(i64),
    /// A string, ordered by its UTF-8 bytes.
    Str(String),
}

/// Which side of a value a column must fall on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RangeSide {
    /// `column > value` or `column >= value`.
    Lower,
    /// `column < value` or `column <= value`.
    Upper,
}

/// One comparison against a literal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RangeBound {
    pub(crate) side: RangeSide,
    pub(crate) value: RangeValue,
    pub(crate) inclusive: bool,
}

/// The conjunction of comparisons a pushed-down predicate places on one stored
/// property column. NULL satisfies none of them.
///
/// The range only chooses what to decode: the predicate is kept above the
/// scan and decides every row that is read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PropertyRange {
    /// Property column name.
    pub(crate) column: String,
    /// Every bound the column must satisfy.
    pub(crate) bounds: Vec<RangeBound>,
}

impl PropertyRange {
    /// Whether values within `[min, max]` can satisfy every bound.
    fn admits_interval<T: Ord + ?Sized>(
        &self,
        min: &T,
        max: &T,
        value_of: impl Fn(&RangeValue) -> Option<&T>,
    ) -> bool {
        self.bounds.iter().all(|bound| {
            let Some(value) = value_of(&bound.value) else {
                // A bound of another type orders nothing here.
                return true;
            };
            match (bound.side, bound.inclusive) {
                (RangeSide::Lower, true) => max >= value,
                (RangeSide::Lower, false) => max > value,
                (RangeSide::Upper, true) => min <= value,
                (RangeSide::Upper, false) => min < value,
            }
        })
    }
}

/// Which row groups of a fragment a read decodes.
#[derive(Clone, Copy)]
pub(crate) enum RowGroupSelection<'a> {
    /// Every row group.
    All,
    /// Row groups whose UUID range can hold one of these UUIDs.
    Uuids(&'a dyn crate::uuid_set::UuidMembership),
    /// Row groups whose statistics admit the equality.
    Equals(&'a PropertyEquality),
}

impl RowGroupSelection<'_> {
    /// The selected row-group indices, or `None` for all of them.
    pub(crate) fn select(
        &self,
        metadata: &ParquetMetaData,
        uuid_field: &str,
    ) -> Option<Vec<usize>> {
        let keep: Box<dyn Fn(&RowGroupMetaData) -> bool + '_> = match self {
            Self::All => return None,
            Self::Uuids(targets) => {
                let leaf = column_leaf(metadata, uuid_field)?;
                Box::new(move |group| group_may_hold_uuid(group, leaf, *targets))
            }
            Self::Equals(equality) => {
                // A fragment without the column has no value to compare, and
                // every row of it is NULL.
                let Some(leaf) = column_leaf(metadata, &equality.column) else {
                    return Some(Vec::new());
                };
                Box::new(move |group| group_may_equal(group, leaf, &equality.value))
            }
        };
        Some(
            metadata
                .row_groups()
                .iter()
                .enumerate()
                .filter(|(_, group)| keep(group))
                .map(|(index, _)| index)
                .collect(),
        )
    }
}

/// The leaf index of a root-level primitive column.
pub(crate) fn column_leaf(metadata: &ParquetMetaData, name: &str) -> Option<usize> {
    let schema = metadata.file_metadata().schema_descr();
    (0..schema.num_columns()).find(|&index| schema.column(index).path().parts() == [name])
}

/// The smallest and largest UUID of a fragment, from its row-group statistics.
/// `None` when any row group lacks them.
pub(crate) fn uuid_range(
    metadata: &ParquetMetaData,
    uuid_field: &str,
) -> Option<([u8; 16], [u8; 16])> {
    let leaf = column_leaf(metadata, uuid_field)?;
    let mut range: Option<([u8; 16], [u8; 16])> = None;
    for group in metadata.row_groups() {
        let (min, max) = uuid_bounds(group, leaf)?;
        range = Some(match range {
            None => (min, max),
            Some((low, high)) => (low.min(min), high.max(max)),
        });
    }
    range
}

/// The smallest and largest UUID of one row group, from its statistics.
pub(super) fn uuid_bounds(group: &RowGroupMetaData, leaf: usize) -> Option<([u8; 16], [u8; 16])> {
    let statistics = group.column(leaf).statistics()?;
    if statistics.is_min_max_deprecated() {
        return None;
    }
    let min = <[u8; 16]>::try_from(statistics.min_bytes_opt()?).ok()?;
    let max = <[u8; 16]>::try_from(statistics.max_bytes_opt()?).ok()?;
    Some((min, max))
}

fn group_may_hold_uuid(
    group: &RowGroupMetaData,
    leaf: usize,
    targets: &dyn crate::uuid_set::UuidMembership,
) -> bool {
    uuid_bounds(group, leaf).is_none_or(|(min, max)| targets.may_contain_in_range(&min, &max))
}

/// Whether `targets` holds every UUID that can fall in each of `groups`, so no
/// row of them needs a per-row membership decision. A group without statistics
/// is never covered.
pub(crate) fn groups_fully_covered(
    metadata: &ParquetMetaData,
    uuid_field: &str,
    groups: &[usize],
    targets: &dyn crate::uuid_set::UuidMembership,
) -> bool {
    let Some(leaf) = column_leaf(metadata, uuid_field) else {
        return false;
    };
    groups.iter().all(|&group| {
        uuid_bounds(metadata.row_group(group), leaf)
            .is_some_and(|(min, max)| targets.contains_every_in_range(&min, &max))
    })
}

/// Whether a fragment can hold a row with `value` in the column.
pub(crate) fn fragment_may_equal(metadata: &ParquetMetaData, equality: &PropertyEquality) -> bool {
    let Some(leaf) = column_leaf(metadata, &equality.column) else {
        return false;
    };
    metadata
        .row_groups()
        .iter()
        .any(|group| group_may_equal(group, leaf, &equality.value))
}

fn group_may_equal(group: &RowGroupMetaData, leaf: usize, value: &EqualityValue) -> bool {
    let Some(statistics) = group.column(leaf).statistics() else {
        return true;
    };
    if statistics.is_min_max_deprecated() {
        return true;
    }
    if statistics
        .null_count_opt()
        .is_some_and(|nulls| i64::try_from(nulls).is_ok_and(|nulls| nulls >= group.num_rows()))
    {
        // Every slot is NULL, and NULL equals nothing.
        return false;
    }
    match (value, statistics) {
        (EqualityValue::Int(wanted), Statistics::Int64(stats)) => {
            match (stats.min_opt(), stats.max_opt()) {
                (Some(min), Some(max)) => min <= wanted && wanted <= max,
                _ => true,
            }
        }
        (EqualityValue::Str(wanted), Statistics::ByteArray(stats)) => {
            match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                (Some(min), Some(max)) => min <= wanted.as_bytes() && wanted.as_bytes() <= max,
                _ => true,
            }
        }
        (EqualityValue::Bool(wanted), Statistics::Boolean(stats)) => {
            match (stats.min_opt(), stats.max_opt()) {
                (Some(min), Some(max)) => min <= wanted && wanted <= max,
                _ => true,
            }
        }
        _ => true,
    }
}

/// Whether a row group can hold a row whose `leaf` column satisfies `range`. A
/// missing, deprecated or non-comparable statistic keeps the row group; so does
/// any group the statistics cannot rule out, because pruning must never remove
/// a row the predicate admits.
pub(crate) fn group_may_satisfy_range(
    group: &RowGroupMetaData,
    leaf: usize,
    range: &PropertyRange,
) -> bool {
    let Some(statistics) = group.column(leaf).statistics() else {
        return true;
    };
    if statistics
        .null_count_opt()
        .is_some_and(|nulls| i64::try_from(nulls).is_ok_and(|nulls| nulls >= group.num_rows()))
    {
        // Every slot is NULL, and NULL satisfies no comparison. This comes
        // before the minimum and maximum checks: a group with no value has
        // none, and the writer marks such statistics as deprecated.
        return false;
    }
    if statistics.is_min_max_deprecated() {
        return true;
    }
    match statistics {
        Statistics::Int64(stats) => match (stats.min_opt(), stats.max_opt()) {
            (Some(min), Some(max)) => range.admits_interval(min, max, |value| match value {
                RangeValue::Int(value) => Some(value),
                RangeValue::Str(_) => None,
            }),
            _ => true,
        },
        Statistics::ByteArray(stats) => match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
            // Truncated bounds still bracket the column: the stored minimum is
            // a prefix, never above the true minimum, and the stored maximum
            // is rounded up, never below the true maximum.
            (Some(min), Some(max)) => range.admits_interval(min, max, |value| match value {
                RangeValue::Str(value) => Some(value.as_bytes()),
                RangeValue::Int(_) => None,
            }),
            _ => true,
        },
        _ => true,
    }
}

/// The row groups of each fragment a range read decodes.
///
/// A row group whose statistics cannot satisfy the range is only skipped when
/// skipping it cannot let an older row it shadows reach the result. Fragments
/// are walked from oldest to newest. A group is kept when its statistics admit
/// the range, or when its UUID span overlaps a kept group of an older fragment:
/// then it may hold the newest snapshot (a changed value, or a tombstone) of a
/// UUID an older kept group would otherwise contribute stale. An older group
/// that is skipped contributes nothing, so a newer group is never kept on its
/// account, and a skipped group never hides a newer snapshot: newer groups are
/// decided on their own. The filter above the scan still decides every row that
/// is read, so a kept group costs only its decode.
pub(crate) struct RangePruning {
    kept: BTreeMap<PropertyFragmentId, Vec<bool>>,
}

type UuidSpan = ([u8; 16], [u8; 16]);

/// The union of the UUID spans of kept groups of older fragments.
#[derive(Default)]
struct KeptSpans {
    /// A kept group without UUID statistics may hold any UUID.
    unbounded: bool,
    /// Sorted, non-overlapping spans.
    merged: Vec<UuidSpan>,
}

impl KeptSpans {
    fn overlaps(&self, span: Option<UuidSpan>) -> bool {
        if self.unbounded {
            return true;
        }
        let Some((min, max)) = span else {
            // The group may hold any UUID.
            return !self.merged.is_empty();
        };
        // The first merged span ending at or after `min` is the only one that
        // can start at or before `max`.
        let index = self.merged.partition_point(|(_, end)| *end < min);
        self.merged
            .get(index)
            .is_some_and(|(start, _)| *start <= max)
    }

    fn extend(&mut self, spans: Vec<Option<UuidSpan>>) {
        for span in spans {
            match span {
                None => self.unbounded = true,
                Some(span) => self.merged.push(span),
            }
        }
        self.merged.sort_unstable();
        let mut merged: Vec<UuidSpan> = Vec::with_capacity(self.merged.len());
        for (start, end) in self.merged.drain(..) {
            match merged.last_mut() {
                Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
                _ => merged.push((start, end)),
            }
        }
        self.merged = merged;
    }
}

impl RangePruning {
    /// Decide the row groups of `fragments`, given in any order with the footer
    /// metadata of each.
    pub(crate) fn new<'a>(
        range: &PropertyRange,
        uuid_field: &str,
        fragments: impl IntoIterator<Item = (PropertyFragmentId, &'a ParquetMetaData)>,
    ) -> Self {
        let mut ordered: Vec<_> = fragments.into_iter().collect();
        ordered.sort_unstable_by_key(|(id, _)| *id);
        let mut older = KeptSpans::default();
        let mut kept = BTreeMap::new();
        for (id, metadata) in ordered {
            let value_leaf = column_leaf(metadata, &range.column);
            let uuid_leaf = column_leaf(metadata, uuid_field);
            let mut flags = Vec::with_capacity(metadata.num_row_groups());
            let mut spans = Vec::new();
            for group in metadata.row_groups() {
                let span = uuid_leaf.and_then(|leaf| uuid_bounds(group, leaf));
                // A fragment without the column holds NULL in every row.
                let admits =
                    value_leaf.is_some_and(|leaf| group_may_satisfy_range(group, leaf, range));
                let keep = admits || older.overlaps(span);
                if keep {
                    spans.push(span);
                }
                flags.push(keep);
            }
            // Groups of one fragment hold distinct UUIDs, so they only shadow
            // the fragments after this one.
            older.extend(spans);
            kept.insert(id, flags);
        }
        Self { kept }
    }

    /// Whether any row group of the fragment is decoded. A fragment this
    /// pruning did not see is read in full.
    pub(crate) fn reads_fragment(&self, id: PropertyFragmentId) -> bool {
        self.kept
            .get(&id)
            .is_none_or(|flags| flags.iter().any(|keep| *keep))
    }

    /// Narrow a row-group selection of a fragment to the groups this pruning
    /// keeps. `selected` is `None` for every group.
    pub(crate) fn restrict(
        &self,
        id: PropertyFragmentId,
        selected: Option<Vec<usize>>,
        row_groups: usize,
    ) -> Result<Option<Vec<usize>>, GfError> {
        let Some(flags) = self.kept.get(&id) else {
            return Ok(selected);
        };
        if flags.len() != row_groups {
            return Err(corrupt(
                "property fragment row groups changed after its footer was read",
            ));
        }
        Ok(Some(match selected {
            Some(groups) => groups.into_iter().filter(|group| flags[*group]).collect(),
            None => (0..row_groups).filter(|group| flags[*group]).collect(),
        }))
    }
}

/// Whether the statistics prove a row group holds no tombstone: the tombstone
/// column is absent (a fragment from before tombstones) or its largest value is
/// `false` and no slot is NULL.
pub(crate) fn group_has_no_tombstone(
    metadata: &ParquetMetaData,
    group: usize,
    tombstone_field: &str,
) -> bool {
    let Some(leaf) = column_leaf(metadata, tombstone_field) else {
        return true;
    };
    let group = metadata.row_group(group);
    match group.column(leaf).statistics() {
        Some(statistics @ Statistics::Boolean(stats)) if !statistics.is_min_max_deprecated() => {
            stats.max_opt() == Some(&false) && stats.null_count_opt() == Some(0)
        }
        _ => false,
    }
}
