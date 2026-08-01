//! Rust-owned path node-selector resolution.
#![allow(dead_code, reason = "consumed by the ordered M18 path vertical slices")]

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, UInt32Array};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

use super::{GfError, GraphForge, NodeSelector, PropValue};

const MAX_SELECTOR_ROWS: usize = 1_000_000;

impl GraphForge {
    /// Resolve one typed selector to a UUID without invoking Cypher or an algorithm.
    pub(crate) fn resolve_node_selector(&self, selector: &NodeSelector) -> Result<Uuid, GfError> {
        match selector {
            NodeSelector::Uuid(uuid) => self.require_node(*uuid),
            NodeSelector::Handle(handle) => {
                if !handle.belongs_to(&self.identity) {
                    return Err(validation("node handle belongs to another graph instance"));
                }
                self.require_node(handle.uuid)
            }
            NodeSelector::Match {
                label,
                property,
                value,
            } => self.resolve_property_match(label, property, value),
        }
    }

    fn require_node(&self, uuid: Uuid) -> Result<Uuid, GfError> {
        if self.node_uuids(None)?.contains(&uuid) {
            Ok(uuid)
        } else {
            Err(validation("node selector matched no nodes"))
        }
    }

    fn resolve_property_match(
        &self,
        label: &str,
        property: &str,
        value: &PropValue,
    ) -> Result<Uuid, GfError> {
        validate_name("label", label)?;
        validate_name("property", property)?;
        let expected = selector_literal(value)?;
        let Some(label_id) = self.label_id(label) else {
            return Err(validation("node selector matched no nodes"));
        };
        let candidates = self.node_uuids(Some(label_id))?;
        let mut observed = HashMap::new();
        let mut matches = HashSet::new();
        let mut scanned = 0usize;
        for stem in graphforge_storage::list_property_stems(&self.dir) {
            for (bytes, properties) in
                graphforge_storage::read_node_property_rows(&self.dir, &stem)?
            {
                scanned += 1;
                if scanned > MAX_SELECTOR_ROWS {
                    return Err(validation("node selector property scan exceeds row limit"));
                }
                let uuid = Uuid::from_bytes(bytes);
                let Some(actual) = properties.get(property) else {
                    continue;
                };
                if candidates.contains(&uuid) {
                    if observed.insert(uuid, actual.clone()).is_some() {
                        return Err(validation("node selector found duplicate property rows"));
                    }
                    if actual == &expected {
                        matches.insert(uuid);
                    }
                }
            }
        }
        match matches.len() {
            0 => Err(validation("node selector matched no nodes")),
            1 => Ok(*matches.iter().next().expect("one match")),
            count => Err(validation(format!(
                "node selector matched {count} nodes and is ambiguous"
            ))),
        }
    }

    fn label_id(&self, label: &str) -> Option<u32> {
        self.ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label).map(|id| id.0))
            .or_else(|| {
                self.runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned")
                    .entity_type_names_with_ids()
                    .find_map(|(id, name)| (name == label).then_some(id.0))
            })
    }

    fn node_uuids(&self, label_id: Option<u32>) -> Result<HashSet<Uuid>, GfError> {
        let mut uuids = HashSet::new();
        let mut scanned = 0usize;
        for batch in graphforge_storage::read_nodes(&self.dir)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            let uuid_column = batch
                .column_by_name("node_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| validation("topology has malformed node_uuid data"))?;
            let labels = batch
                .column_by_name("type_ids")
                .and_then(|column| column.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| validation("topology has malformed type_ids data"))?;
            for row in 0..batch.num_rows() {
                scanned += 1;
                if scanned > MAX_SELECTOR_ROWS {
                    return Err(validation("node selector topology scan exceeds row limit"));
                }
                if uuid_column.is_null(row) || labels.is_null(row) {
                    return Err(validation("topology contains null node identity data"));
                }
                let uuid = Uuid::from_slice(uuid_column.value(row))
                    .map_err(|_| validation("topology has malformed node_uuid data"))?;
                let label_values = labels.value(row);
                let label_values = label_values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| validation("topology has malformed type_ids data"))?;
                let selected =
                    label_id.is_none_or(|wanted| label_values.values().contains(&wanted));
                if selected && !uuids.insert(uuid) {
                    return Err(validation("topology contains duplicate node UUIDs"));
                }
            }
        }
        Ok(uuids)
    }
}

fn selector_literal(value: &PropValue) -> Result<IrLiteral, GfError> {
    match value {
        PropValue::Null => Err(validation("node selector value cannot be null")),
        PropValue::Bool(value) => Ok(IrLiteral::Bool(*value)),
        PropValue::Int(value) => Ok(IrLiteral::Int(*value)),
        PropValue::Float(value) if value.is_finite() => Ok(IrLiteral::Float(*value)),
        PropValue::Float(_) => Err(validation("node selector value must be finite")),
        PropValue::Str(value) => Ok(IrLiteral::Str(value.clone())),
        PropValue::List(values) => values
            .iter()
            .map(selector_literal)
            .collect::<Result<Vec<_>, _>>()
            .map(IrLiteral::List),
        _ => Err(validation("unsupported node selector value")),
    }
}

fn validate_name(kind: &str, name: &str) -> Result<(), GfError> {
    if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
        Err(validation(format!("invalid node selector {kind} {name:?}")))
    } else {
        Ok(())
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeHandle;
    use graphforge_core::PathsOptions;

    fn first_uuid(graph: &GraphForge) -> Uuid {
        let batches = graphforge_storage::read_nodes(&graph.dir).unwrap();
        let column = batches[0]
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        Uuid::from_slice(column.value(0)).unwrap()
    }

    fn assert_validation<T>(result: Result<T, GfError>) {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }

    #[test]
    fn equivalent_selectors_resolve_to_one_uuid_without_an_ontology() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();
        let uuid = first_uuid(&graph);
        let handle = NodeHandle::new(uuid, "Person", graph.identity.clone());
        let property = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("Alice".into()),
        };
        assert_eq!(
            graph
                .resolve_node_selector(&NodeSelector::Uuid(uuid))
                .unwrap(),
            uuid
        );
        assert_eq!(
            graph
                .resolve_node_selector(&NodeSelector::Handle(handle))
                .unwrap(),
            uuid
        );
        assert_eq!(graph.resolve_node_selector(&property).unwrap(), uuid);
    }

    #[test]
    fn malformed_missing_ambiguous_and_cross_graph_selectors_are_rejected() {
        assert_validation(NodeSelector::uuid("not-a-uuid"));
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (:Person {name: 'same'}), (:Person {name: 'same'})")
            .unwrap();
        let ambiguous = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("same".into()),
        };
        assert_validation(graph.resolve_node_selector(&ambiguous));
        let missing = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("missing".into()),
        };
        assert_validation(graph.resolve_node_selector(&missing));
        assert_validation(graph.resolve_node_selector(&NodeSelector::Uuid(Uuid::now_v7())));
        let other = GraphForge::new(None).unwrap();
        let foreign = NodeHandle::new(first_uuid(&graph), "Person", graph.identity.clone());
        assert_validation(other.resolve_node_selector(&NodeSelector::Handle(foreign)));

        other.execute("CREATE (:Person {name: 'unique'})").unwrap();
        let uuid = first_uuid(&other);
        let updates = HashMap::from([(
            *uuid.as_bytes(),
            HashMap::from([("name".into(), IrLiteral::Str("unique".into()))]),
        )]);
        graphforge_storage::set_node_properties(&other.dir, "Person", &updates).unwrap();
        let duplicate = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("unique".into()),
        };
        assert_validation(other.resolve_node_selector(&duplicate));
    }

    #[test]
    fn typed_paths_boundary_resolves_both_selectors_before_dispatch() {
        let graph = GraphForge::new(None).unwrap();
        let alice = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Alice".into()))]),
            )
            .unwrap();
        let bob = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Bob".into()))]),
            )
            .unwrap();
        let persisted = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid")
            .unwrap();
        assert_eq!(persisted.stats.rows_produced, 2);
        let source = NodeSelector::Uuid(alice.uuid);
        let target = NodeSelector::Handle(bob);
        let options = PathsOptions::default();
        assert!(graph.paths(&source, Some(&target), options.clone()).is_ok());

        let property = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("Alice".into()),
        };
        assert!(graph.paths(&property, None, options.clone()).is_ok());

        let missing = NodeSelector::Uuid(Uuid::now_v7());
        assert_validation(graph.paths(&source, Some(&missing), options.clone()));
        let other = GraphForge::new(None).unwrap();
        let foreign = NodeHandle::new(alice.uuid, "Person", graph.identity.clone());
        assert_validation(other.paths(&NodeSelector::Handle(foreign), None, options));
    }
}
