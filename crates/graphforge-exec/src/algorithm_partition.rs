//! Validated graph-layer partition mappings for M18 algorithms.
//!
//! Property storage and knowledge-layer projection happen above this boundary.
//! Kernels receive only the immutable, normalized UUID mapping produced here.

use std::collections::{BTreeMap, BTreeSet};

/// One graph-native scalar value resolved for a selected node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PartitionValue {
    /// A string property value, preserved exactly.
    String(String),
    /// An integer property value, normalized to its decimal representation.
    Integer(i64),
    /// A missing Arrow value.
    Null,
    /// A scalar or nested property type that partitions do not support.
    Unsupported(&'static str),
}

/// A canonical partition identifier shared by partition-aware kernels.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PartitionId(String);

impl PartitionId {
    /// The stable string emitted by partition-result schemas.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// A complete immutable mapping for one selected graph projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedPartitionMap {
    assignments: BTreeMap<[u8; 16], PartitionId>,
}

impl ResolvedPartitionMap {
    /// Validate and normalize already-resolved graph property values.
    ///
    /// `selected_nodes` is the entire algorithm projection. Every entry must
    /// refer to exactly one selected UUID, and every selected UUID must have an
    /// entry. Validation completes before a mapping is returned.
    pub(crate) fn try_new(
        selected_nodes: impl IntoIterator<Item = [u8; 16]>,
        values: impl IntoIterator<Item = ([u8; 16], PartitionValue)>,
    ) -> Result<Self, PartitionMappingError> {
        let selected = selected_nodes.into_iter().collect::<BTreeSet<_>>();
        let mut assignments = BTreeMap::new();

        for (node_uuid, value) in values {
            if !selected.contains(&node_uuid) {
                return Err(PartitionMappingError::OutsideProjection { node_uuid });
            }
            if assignments.contains_key(&node_uuid) {
                return Err(PartitionMappingError::DuplicateNode { node_uuid });
            }

            let partition = match value {
                PartitionValue::String(value) => PartitionId(value),
                PartitionValue::Integer(value) => PartitionId(value.to_string()),
                PartitionValue::Null => {
                    return Err(PartitionMappingError::NullValue { node_uuid });
                }
                PartitionValue::Unsupported(value_type) => {
                    return Err(PartitionMappingError::UnsupportedValue {
                        node_uuid,
                        value_type,
                    });
                }
            };
            assignments.insert(node_uuid, partition);
        }

        if let Some(&node_uuid) = selected
            .iter()
            .find(|node_uuid| !assignments.contains_key(*node_uuid))
        {
            return Err(PartitionMappingError::MissingValue { node_uuid });
        }

        Ok(Self { assignments })
    }

    /// Look up one selected node's normalized partition.
    #[must_use]
    pub(crate) fn get(&self, node_uuid: &[u8; 16]) -> Option<&PartitionId> {
        self.assignments.get(node_uuid)
    }

    /// Iterate in canonical UUID order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8; 16], &PartitionId)> {
        self.assignments.iter()
    }
}

/// Atomic validation failures at the graph-to-kernel partition boundary.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PartitionMappingError {
    /// A selected node had no resolved property value.
    #[error("selected node {node_uuid:?} is missing a partition value")]
    MissingValue { node_uuid: [u8; 16] },
    /// A selected node's property value was NULL.
    #[error("selected node {node_uuid:?} has a NULL partition value")]
    NullValue { node_uuid: [u8; 16] },
    /// A selected node's property type cannot identify a partition.
    #[error("selected node {node_uuid:?} has unsupported partition type {value_type}")]
    UnsupportedValue {
        node_uuid: [u8; 16],
        value_type: &'static str,
    },
    /// The provider emitted a node more than once.
    #[error("selected node {node_uuid:?} has duplicate partition values")]
    DuplicateNode { node_uuid: [u8; 16] },
    /// The provider leaked a node outside the selected projection.
    #[error("partition value for node {node_uuid:?} is outside the selected projection")]
    OutsideProjection { node_uuid: [u8; 16] },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    #[test]
    fn normalizes_strings_and_integers_in_uuid_order() {
        let mapping = ResolvedPartitionMap::try_new(
            [uuid(2), uuid(1)],
            [
                (uuid(2), PartitionValue::Integer(-7)),
                (uuid(1), PartitionValue::String("left".into())),
            ],
        )
        .expect("valid mapping");

        let rows = mapping
            .iter()
            .map(|(node, partition)| (*node, partition.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(rows, [(uuid(1), "left"), (uuid(2), "-7")]);
        assert_eq!(mapping.get(&uuid(2)).map(PartitionId::as_str), Some("-7"));
    }

    #[test]
    fn accepts_an_empty_projection() {
        let mapping = ResolvedPartitionMap::try_new([], []);
        assert_eq!(mapping.expect("empty mapping").iter().count(), 0);
    }

    #[test]
    fn rejects_incomplete_or_null_mappings() {
        assert_eq!(
            ResolvedPartitionMap::try_new([uuid(1)], []),
            Err(PartitionMappingError::MissingValue { node_uuid: uuid(1) })
        );
        assert_eq!(
            ResolvedPartitionMap::try_new([uuid(1)], [(uuid(1), PartitionValue::Null)]),
            Err(PartitionMappingError::NullValue { node_uuid: uuid(1) })
        );
    }

    #[test]
    fn rejects_unsupported_duplicate_and_out_of_projection_values() {
        assert_eq!(
            ResolvedPartitionMap::try_new(
                [uuid(1)],
                [(uuid(1), PartitionValue::Unsupported("Float64"))]
            ),
            Err(PartitionMappingError::UnsupportedValue {
                node_uuid: uuid(1),
                value_type: "Float64",
            })
        );
        assert_eq!(
            ResolvedPartitionMap::try_new(
                [uuid(1)],
                [
                    (uuid(1), PartitionValue::Integer(0)),
                    (uuid(1), PartitionValue::Integer(1)),
                ]
            ),
            Err(PartitionMappingError::DuplicateNode { node_uuid: uuid(1) })
        );
        assert_eq!(
            ResolvedPartitionMap::try_new(
                [uuid(1)],
                [(uuid(2), PartitionValue::String("right".into()))]
            ),
            Err(PartitionMappingError::OutsideProjection { node_uuid: uuid(2) })
        );
    }
}
