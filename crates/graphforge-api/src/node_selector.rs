//! Rust-owned path node-selector resolution.
#![allow(
    dead_code,
    reason = "consumed by the ordered algorithm path vertical slices"
)]

use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_value::EntityTypeId;

use super::{GfError, GraphForge, NodeSelector, PropValue};

const SELECTOR_PARAMETER: &str = "selector_value";

impl GraphForge {
    /// Resolve the optional source and target selectors of one verb call.
    ///
    /// A `Match` selector runs a Cypher read, so the pair resolves under graph
    /// visibility and sees one published generation. UUID and handle selectors
    /// need no lock. Callers that already hold graph visibility (the descriptor
    /// dispatch holds its write permit) must pass only UUID selectors here, as
    /// the read lock is not reentrant.
    pub(crate) fn resolve_endpoint_selectors(
        &self,
        source: Option<&NodeSelector>,
        target: Option<&NodeSelector>,
    ) -> Result<(Option<Uuid>, Option<Uuid>), GfError> {
        let _visibility = [source, target]
            .into_iter()
            .flatten()
            .any(|selector| matches!(selector, NodeSelector::Match { .. }))
            .then(|| self.graph_visibility.read())
            .transpose()?;
        let source = source
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        let target = target
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        Ok((source, target))
    }

    /// Resolve one typed selector to a UUID without invoking an algorithm.
    ///
    /// The caller holds graph visibility (read or write). A `Match` selector
    /// runs the anchored Cypher lookup `MATCH (n:L {p: $v})`, so it costs what
    /// that Cypher anchor costs and uses whatever index the planner offers for
    /// it, instead of scanning the topology and property routes.
    pub(crate) fn resolve_node_selector(&self, selector: &NodeSelector) -> Result<Uuid, GfError> {
        self.graph_visibility.health.check()?;
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

    /// Confirm one node UUID exists. A selector that names one node resolves by
    /// identity lookup in the published node Parquet (row-group and page pruning,
    /// ADR 0057), so it is not bounded by the graph's node count.
    fn require_node(&self, uuid: Uuid) -> Result<Uuid, GfError> {
        let (found, _) = self
            .cached_identity_probe()?
            .probe(graphforge_storage::UuidIndexKind::Node, &[uuid])?;
        if found.first().copied().unwrap_or(false) {
            Ok(uuid)
        } else {
            Err(validation("node selector matched no nodes"))
        }
    }

    /// Resolve `label`/`property`/`value` to the single node it matches. The
    /// lookup stops at the second match, so the only bound is the ambiguity
    /// refusal on matched rows; no scanned-row limit applies.
    fn resolve_property_match(
        &self,
        label: &str,
        property: &str,
        value: &PropValue,
    ) -> Result<Uuid, GfError> {
        validate_name("label", label)?;
        validate_name("property", property)?;
        let expected = selector_literal(value)?;
        if self.label_id(label)?.is_none() {
            return Err(validation("node selector matched no nodes"));
        }
        let query = format!(
            "MATCH (n:`{label}` {{`{property}`: ${SELECTOR_PARAMETER}}}) \
             RETURN n.node_uuid AS node_uuid LIMIT 2"
        );
        let result = self.execute_read_under_visibility(
            &query,
            &HashMap::from([(SELECTOR_PARAMETER.to_owned(), expected)]),
        )?;
        let mut matches = Vec::with_capacity(2);
        for batch in &result.batches {
            let column = batch
                .column_by_name("node_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| validation("node selector lookup returned malformed node_uuid"))?;
            for row in 0..column.len() {
                if column.is_null(row) {
                    return Err(validation("node selector lookup returned a null node_uuid"));
                }
                matches
                    .push(Uuid::from_slice(column.value(row)).map_err(|_| {
                        validation("node selector lookup returned a bad node_uuid")
                    })?);
            }
        }
        match matches.as_slice() {
            [] => Err(validation("node selector matched no nodes")),
            [uuid] => Ok(*uuid),
            _ => Err(validation(
                "node selector matched more than one node and is ambiguous",
            )),
        }
    }

    fn label_id(&self, label: &str) -> Result<Option<EntityTypeId>, GfError> {
        if let Some(id) = self
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label))
        {
            return EntityTypeId::ontology(id)
                .map(Some)
                .map_err(|error| validation(error.to_string()));
        }
        Ok(self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .entity_type_names_with_ids()
            .find_map(|(id, name)| (name == label).then_some(EntityTypeId::runtime(id))))
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
        PropValue::Temporal(value) => {
            value.validate()?;
            Ok(crate::construction::temporal_literal(value))
        }
        PropValue::List(values) => values
            .iter()
            .map(selector_literal)
            .collect::<Result<Vec<_>, _>>()
            .map(IrLiteral::List),
        _ => Err(validation("unsupported node selector value")),
    }
}

fn validate_name(kind: &str, name: &str) -> Result<(), GfError> {
    // Cypher backtick identifiers have no escape for a backtick.
    if name.is_empty() || name.trim() != name || name.chars().any(|c| c.is_control() || c == '`') {
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
        let batches = graphforge_storage::read_nodes(&graph.dir()).unwrap();
        let column = batches[0]
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        Uuid::from_slice(column.value(0)).unwrap()
    }

    #[track_caller]
    fn assert_validation<T>(result: Result<T, GfError>) {
        assert!(
            matches!(&result, Err(GfError::Validation(_))),
            "expected validation refusal; error={:?}",
            result.as_ref().err()
        );
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
    fn reserved_label_property_selector_preserves_exact_match() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (:CON {name: 'Alice'}), (:con {name: 'Alice'})")
            .unwrap();
        let upper = graph
            .resolve_node_selector(&NodeSelector::Match {
                label: "CON".into(),
                property: "name".into(),
                value: PropValue::Str("Alice".into()),
            })
            .unwrap();
        let lower = graph
            .resolve_node_selector(&NodeSelector::Match {
                label: "con".into(),
                property: "name".into(),
                value: PropValue::Str("Alice".into()),
            })
            .unwrap();
        assert_ne!(upper, lower);
    }

    #[test]
    fn uuid_selector_resolves_through_the_published_parquet() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();
        let uuid = first_uuid(&graph);
        assert!(graph.identity_probe.state.lock().unwrap().current.is_none());
        assert_eq!(
            graph
                .resolve_node_selector(&NodeSelector::Uuid(uuid))
                .unwrap(),
            uuid
        );
        // The lookup opened the Parquet probe; no topology scan was needed.
        assert!(graph.identity_probe.state.lock().unwrap().current.is_some());
        assert_validation(graph.resolve_node_selector(&NodeSelector::Uuid(Uuid::now_v7())));

        // A new generation replaces the cached probe, and the new node resolves.
        graph.execute("CREATE (:Person {name: 'Bob'})").unwrap();
        let both = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid")
            .unwrap();
        let column = both.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..column.len() {
            let found = Uuid::from_slice(column.value(row)).unwrap();
            assert_eq!(
                graph
                    .resolve_node_selector(&NodeSelector::Uuid(found))
                    .unwrap(),
                found
            );
        }
    }

    #[test]
    fn clear_and_repopulate_to_the_same_generation_never_reuses_the_cached_index() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'old'})").unwrap();
        let old = first_uuid(&graph);
        let generation = graphforge_storage::read_topology_generation(&graph.dir()).unwrap();
        // Open the probe cache at this generation.
        assert_eq!(
            graph
                .resolve_node_selector(&NodeSelector::Uuid(old))
                .unwrap(),
            old
        );
        assert!(graph.identity_probe.state.lock().unwrap().current.is_some());

        graph.clear().unwrap();
        graph.execute("CREATE (:Person {name: 'new'})").unwrap();
        // clear() resets the generation counter, so it repeats the earlier value.
        assert_eq!(
            graphforge_storage::read_topology_generation(&graph.dir()).unwrap(),
            generation
        );
        let new = first_uuid(&graph);
        assert_ne!(new, old);
        assert_eq!(
            graph
                .resolve_node_selector(&NodeSelector::Uuid(new))
                .unwrap(),
            new
        );
        assert_validation(graph.resolve_node_selector(&NodeSelector::Uuid(old)));
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
        graphforge_storage::set_node_properties(&other.dir(), "Person", &updates).unwrap();
        let duplicate = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("unique".into()),
        };
        // Direct storage edits do not replace an already admitted facade view.
        assert_eq!(other.resolve_node_selector(&duplicate).unwrap(), uuid);
    }

    #[test]
    fn interactive_complex_13_selector_shape_returns_the_uuid_selector_paths() {
        // IC13 names both endpoints as `Person {id: <int>}` and asks for an
        // undirected BFS over KNOWS.
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {id: 933}), (b:Person {id: 4139}), (c:Person {id: 6597}), \
                 (d:Person {id: 8333}), (:Person {id: 1}), \
                 (a)-[:KNOWS]->(b), (c)-[:KNOWS]->(b), (c)-[:KNOWS]->(d)",
            )
            .unwrap();
        let person = |id: i64| NodeSelector::Match {
            label: "Person".into(),
            property: "id".into(),
            value: PropValue::Int(id),
        };
        let uuid_of = |id: i64| {
            let rows = graph
                .execute(&format!("MATCH (n:Person {{id: {id}}}) RETURN n.node_uuid"))
                .unwrap();
            let column = rows.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Uuid::from_slice(column.value(0)).unwrap()
        };
        let options = PathsOptions {
            by: crate::PathAlgorithm::Bfs,
            via: Some("KNOWS".into()),
            directed: false,
            ..PathsOptions::default()
        };
        let by_match = graph
            .paths(&person(933), Some(&person(8333)), options.clone())
            .unwrap();
        let by_uuid = graph
            .paths(
                &NodeSelector::Uuid(uuid_of(933)),
                Some(&NodeSelector::Uuid(uuid_of(8333))),
                options.clone(),
            )
            .unwrap();
        assert_eq!(by_match, by_uuid);
        assert_eq!(by_match.num_rows(), 1);
        // The path is a -> b <- c -> d, four nodes long.
        let path = by_match
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(path.len(), 4);
        assert_validation(graph.paths(&person(2), Some(&person(8333)), options));
    }

    #[test]
    fn selector_names_that_cypher_cannot_quote_are_refused() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();
        for (label, property) in [("Per`son", "name"), ("Person", "na`me")] {
            assert_validation(graph.resolve_node_selector(&NodeSelector::Match {
                label: label.into(),
                property: property.into(),
                value: PropValue::Str("Alice".into()),
            }));
        }
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
