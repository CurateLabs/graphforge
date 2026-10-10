//! Statistics-pruned selection for property reads (#1931).
//!
//! A fragment's Parquet footer carries per-row-group statistics for the UUID
//! key and for every primitive property column. They prune two things without
//! touching a data page: row groups that cannot hold a requested UUID, and
//! fragments or row groups that cannot hold a requested property value.
//!
//! Pruning only ever removes work that cannot change the answer. A missing,
//! deprecated or non-comparable statistic keeps its row group.

use arrow::datatypes::DataType;
use graphforge_ir::IrLiteral;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

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

fn uuid_bounds(group: &RowGroupMetaData, leaf: usize) -> Option<([u8; 16], [u8; 16])> {
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
