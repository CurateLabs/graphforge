//! Rust-owned construction methods on the public facade.

use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray, StructArray};
use graphforge_core::uuid::Uuid;

use super::{EdgeHandle, GfError, GraphForge, IrLiteral, NodeHandle, PropValue};

impl GraphForge {
    /// Add one labelled node through the canonical transactional CREATE path.
    ///
    /// The returned handle exposes the generated UUIDv7 and is owned by this
    /// exact `GraphForge` instance. Property names are sorted before query
    /// construction so identical inputs take a deterministic bind/write path.
    ///
    /// # Errors
    /// Returns a structured validation, bind, plan, execution, or storage error
    /// without committing a partial node.
    pub fn add_node(
        &self,
        label: &str,
        props: &HashMap<String, PropValue>,
    ) -> Result<NodeHandle, GfError> {
        validate_identifier("label", label)?;

        let mut properties = props.iter().collect::<Vec<_>>();
        properties.sort_unstable_by(|left, right| left.0.cmp(right.0));

        let strict_owner = match (self.ontology_mode, self.ontology.as_ref()) {
            (graphforge_core::OntologyMode::Strict, Some(ontology)) => ontology
                .entity_type_id(label)
                .map(|owner| (ontology, owner)),
            _ => None,
        };

        let mut params = HashMap::with_capacity(properties.len());
        let mut entries = Vec::with_capacity(properties.len());
        for (index, (name, value)) in properties.into_iter().enumerate() {
            validate_identifier("property", name)?;
            if matches!(
                name.as_str(),
                "node_uuid" | "node_id" | "type_id" | "type_ids"
            ) {
                return Err(validation(format!(
                    "property {name:?} is a reserved node topology field"
                )));
            }
            if let Some((ontology, owner)) = strict_owner
                && !ontology.has_entity_property(owner, name)
            {
                return Err(validation(format!(
                    "property {name:?} is not declared for strict entity type {label:?}"
                )));
            }
            let parameter = format!("gf_add_node_{index}");
            params.insert(parameter.clone(), prop_literal(value)?);
            entries.push(format!("`{name}`: ${parameter}"));
        }

        let property_map = if entries.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", entries.join(", "))
        };
        let query = format!("CREATE (node:`{label}`{property_map}) RETURN node");
        let result = self.execute_with_params(&query, &params)?;
        let uuid = created_uuid(&result)?;
        Ok(NodeHandle::new(uuid, label, self.identity.clone()))
    }

    /// Add one directed edge through the graph writer and composite project
    /// publication path.
    ///
    /// # Errors
    /// Returns validation for foreign handles, malformed identifiers, or
    /// reserved properties and preserves the prior committed generation on
    /// write/publication failure.
    pub fn add_edge(
        &self,
        src: &NodeHandle,
        rel_type: &str,
        dst: &NodeHandle,
        props: &HashMap<String, PropValue>,
    ) -> Result<EdgeHandle, GfError> {
        validate_identifier("relationship type", rel_type)?;
        let src_uuid =
            self.resolve_node_selector(&graphforge_core::NodeSelector::Handle(src.clone()))?;
        let dst_uuid =
            self.resolve_node_selector(&graphforge_core::NodeSelector::Handle(dst.clone()))?;
        let mut properties = HashMap::with_capacity(props.len());
        for (name, value) in props {
            validate_identifier("property", name)?;
            if matches!(
                name.as_str(),
                "edge_uuid" | "edge_id" | "src_uuid" | "src_id" | "dst_uuid" | "dst_id"
            ) {
                return Err(validation(format!(
                    "property {name:?} is a reserved edge topology field"
                )));
            }
            properties.insert(name.clone(), prop_literal(value)?);
        }

        // Same-instance write admission and visibility must precede snapshot
        // selection, endpoint registration against live topology, surrogate
        // allocation, flush, and publication (see #704).
        let _visibility = self.graph_visibility.lock()?;
        let prior = crate::graph_snapshot::capture(&self.dir)?;
        let expected_generation = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let now = (self.clock.lock().expect("clock lock poisoned"))()?;
        let mut writer =
            graphforge_storage::GraphWriter::open_at(&self.dir, self.ontology_mode, now)?;
        register_endpoint(&mut writer, &self.dir, src_uuid)?;
        register_endpoint(&mut writer, &self.dir, dst_uuid)?;
        let edge_uuid = Uuid::now_v7();
        writer.create_edge(edge_uuid, rel_type, &src_uuid, &dst_uuid)?;
        if !properties.is_empty() {
            writer.set_edge_properties(&edge_uuid, Some(rel_type), properties)?;
        }
        writer.flush()?;
        let receipt = graphforge_exec::MutationReceipt {
            effects: vec![graphforge_exec::MutationEffect {
                kind: graphforge_exec::MutationKind::CreateEdge,
                inputs: vec![
                    graphforge_exec::MutationSubject {
                        uuid: src_uuid.into_bytes(),
                        kind: graphforge_exec::MutationSubjectKind::Node,
                    },
                    graphforge_exec::MutationSubject {
                        uuid: dst_uuid.into_bytes(),
                        kind: graphforge_exec::MutationSubjectKind::Node,
                    },
                ],
                outputs: vec![graphforge_exec::MutationSubject {
                    uuid: edge_uuid.into_bytes(),
                    kind: graphforge_exec::MutationSubjectKind::Edge,
                }],
            }],
        };
        if let Err(error) = self.publish_graph_mutation(&receipt) {
            let still_prior = *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned")
                == expected_generation;
            if still_prior {
                crate::graph_snapshot::restore(&prior.bytes, &self.dir)?;
                self.adjacency_provider.invalidate();
            }
            return Err(error);
        }
        self.adjacency_provider.invalidate();
        Ok(EdgeHandle::new(edge_uuid, rel_type))
    }
}

fn register_endpoint(
    writer: &mut graphforge_storage::GraphWriter,
    dir: &std::path::Path,
    uuid: Uuid,
) -> Result<(), GfError> {
    for batch in graphforge_storage::read_nodes(dir)
        .map_err(|error| GfError::Storage(format!("failed to read node topology: {error}")))?
    {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Storage("node topology has malformed UUID column".into()))?;
        let ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
            .ok_or_else(|| GfError::Storage("node topology has malformed ID column".into()))?;
        for row in 0..batch.num_rows() {
            if uuids.value(row) == uuid.as_bytes() {
                writer.register_existing_node(uuid, ids.value(row));
                return Ok(());
            }
        }
    }
    Err(validation("edge endpoint is not present in this graph"))
}

fn validate_identifier(kind: &str, name: &str) -> Result<(), GfError> {
    let mut characters = name.chars();
    let valid = characters
        .next()
        .is_some_and(|first| first == '_' || first.is_alphabetic())
        && characters.all(|character| character == '_' || character.is_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(validation(format!("invalid node {kind} {name:?}")))
    }
}

pub(crate) fn prop_literal(value: &PropValue) -> Result<IrLiteral, GfError> {
    match value {
        PropValue::Null => Ok(IrLiteral::Null),
        PropValue::Bool(value) => Ok(IrLiteral::Bool(*value)),
        PropValue::Int(value) => Ok(IrLiteral::Int(*value)),
        PropValue::Float(value) => Ok(IrLiteral::Float(*value)),
        PropValue::Str(value) => Ok(IrLiteral::Str(value.clone())),
        PropValue::List(values) => values
            .iter()
            .map(prop_literal)
            .collect::<Result<Vec<_>, _>>()
            .map(IrLiteral::List),
        _ => Err(validation("unsupported node property value")),
    }
}

fn created_uuid(result: &graphforge_exec::ExecutionResult) -> Result<Uuid, GfError> {
    if result.stats.rows_produced != 1
        || result
            .side_effects
            .as_ref()
            .is_none_or(|effects| effects.nodes_created != 1)
    {
        return Err(GfError::Execution(
            "add_node CREATE did not produce exactly one node".into(),
        ));
    }
    let batch = result
        .batches
        .first()
        .ok_or_else(|| GfError::Execution("add_node CREATE returned no batch".into()))?;
    let node = batch
        .column_by_name("node")
        .and_then(|column| column.as_any().downcast_ref::<StructArray>())
        .ok_or_else(|| GfError::Execution("add_node CREATE returned malformed node data".into()))?;
    let uuids = node
        .column_by_name("node_uuid")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| GfError::Execution("add_node CREATE returned malformed UUID data".into()))?;
    if uuids.len() != 1 || uuids.is_null(0) {
        return Err(GfError::Execution(
            "add_node CREATE returned malformed UUID data".into(),
        ));
    }
    Uuid::from_slice(uuids.value(0))
        .map_err(|_| GfError::Execution("add_node CREATE returned malformed UUID data".into()))
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};

    fn properties() -> HashMap<String, PropValue> {
        HashMap::from([
            ("name".into(), PropValue::Str("Alice".into())),
            ("score".into(), PropValue::Int(7)),
        ])
    }

    #[test]
    fn in_memory_node_is_readable_and_owned_by_its_graph() {
        let graph = GraphForge::new(None).unwrap();
        let handle = graph.add_node("Person", &properties()).unwrap();
        assert_eq!(handle.uuid.get_version_num(), 7);

        assert_eq!(
            graph
                .resolve_node_selector(&graphforge_core::NodeSelector::Handle(handle.clone()))
                .unwrap(),
            handle.uuid
        );
        let result = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS id, n.name AS name, n.score AS score")
            .unwrap();
        let batch = &result.batches[0];
        assert_eq!(
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            handle.uuid.as_bytes()
        );
        assert_eq!(
            batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Alice"
        );
        assert_eq!(
            batch
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );

        let other = GraphForge::new(None).unwrap();
        assert!(
            other
                .resolve_node_selector(&graphforge_core::NodeSelector::Handle(handle))
                .is_err()
        );
    }

    #[test]
    fn persistent_node_survives_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let uuid = {
            let graph = GraphForge::new(dir.path().to_str()).unwrap();
            graph.add_node("Person", &properties()).unwrap().uuid
        };
        let reopened = GraphForge::new(dir.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .resolve_node_selector(&graphforge_core::NodeSelector::Uuid(uuid))
                .unwrap(),
            uuid
        );
    }

    #[test]
    fn persistent_edge_survives_reopen_with_properties() {
        let dir = tempfile::TempDir::new().unwrap();
        let (src_uuid, dst_uuid, edge_uuid) = {
            let graph = GraphForge::new(dir.path().to_str()).unwrap();
            let src = graph.add_node("Person", &properties()).unwrap();
            let dst = graph.add_node("Person", &HashMap::new()).unwrap();
            let edge = graph
                .add_edge(
                    &src,
                    "KNOWS",
                    &dst,
                    &HashMap::from([("since".into(), PropValue::Int(2026))]),
                )
                .unwrap();
            (src.uuid, dst.uuid, edge.uuid)
        };
        let reopened = GraphForge::new(dir.path().to_str()).unwrap();
        let result = reopened
            .execute(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
                 RETURN a.node_uuid AS src, r.edge_uuid AS edge, \
                 b.node_uuid AS dst, r.since AS since",
            )
            .unwrap();
        let batch = &result.batches[0];
        assert_eq!(
            batch
                .column_by_name("src")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            src_uuid.as_bytes()
        );
        assert_eq!(
            batch
                .column_by_name("edge")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            edge_uuid.as_bytes()
        );
        assert_eq!(
            batch
                .column_by_name("dst")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            dst_uuid.as_bytes()
        );
        assert_eq!(
            batch
                .column_by_name("since")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2026
        );
    }

    #[test]
    fn invalid_input_leaves_catalog_and_topology_unchanged() {
        let graph = GraphForge::new(None).unwrap();
        let catalog_rows = graph
            .runtime_catalog
            .lock()
            .unwrap()
            .to_record_batch()
            .num_rows();
        let generation = graphforge_storage::read_topology_generation(&graph.dir).unwrap();

        for (label, props) in [
            ("", properties()),
            ("9Label", properties()),
            ("My-Label", properties()),
            ("My Label", properties()),
            (
                "Person",
                HashMap::from([("node_uuid".into(), PropValue::Str("shadow".into()))]),
            ),
        ] {
            assert!(matches!(
                graph.add_node(label, &props),
                Err(GfError::Validation(_))
            ));
        }
        assert_eq!(
            graph
                .runtime_catalog
                .lock()
                .unwrap()
                .to_record_batch()
                .num_rows(),
            catalog_rows
        );
        assert_eq!(
            graphforge_storage::read_topology_generation(&graph.dir).unwrap(),
            generation
        );
        assert_eq!(
            graphforge_storage::read_nodes(&graph.dir)
                .unwrap()
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }

    #[test]
    fn strict_ontology_accepts_declared_and_rejects_unknown_labels_atomically() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut bootstrap = GraphForge::new(dir.path().to_str()).unwrap();
        let ontology_path = dir.path().join(graphforge_core::manifest::ONTOLOGY_FILE);
        std::fs::write(
            &ontology_path,
            "ontology_id: t\nversion: \"v1\"\nentity_types:\n  - name: Person\n    abstract: false\nproperties:\n  - owner: Person\n    name: name\n    type: utf8\n    nullable: true\n  - owner: Person\n    name: score\n    type: int64\n    nullable: true\n",
        )
        .unwrap();
        bootstrap
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(uuid::Uuid::from_u128(719)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: graphforge_core::OntologyMode::Strict,
            })
            .unwrap();
        drop(bootstrap);

        let graph = GraphForge::new(dir.path().to_str()).unwrap();
        let handle = graph.add_node("Person", &properties()).unwrap();
        let generation = graphforge_storage::read_topology_generation(&graph.dir).unwrap();
        let error = graph.add_node("Unknown", &HashMap::new()).unwrap_err();
        assert!(matches!(error, GfError::Bind { .. }));
        assert_eq!(
            graphforge_storage::read_topology_generation(&graph.dir).unwrap(),
            generation
        );
        assert_eq!(
            graph
                .resolve_node_selector(&graphforge_core::NodeSelector::Uuid(handle.uuid))
                .unwrap(),
            handle.uuid
        );
    }

    #[test]
    fn strict_add_node_rejects_undeclared_properties_before_publication() {
        let dir = tempfile::TempDir::new().unwrap();
        let ontology_path = dir.path().join("strict.yaml");
        let mut graph = GraphForge::new(dir.path().to_str()).unwrap();
        std::fs::write(
            &ontology_path,
            "ontology_id: construction\nversion: \"1\"\nentity_types:\n  - name: Asset\n    abstract: false\n  - name: Host\n    abstract: false\n    parent: Asset\nproperties:\n  - owner: Asset\n    name: name\n    type: utf8\n    nullable: false\n  - owner: Host\n    name: hostname\n    type: utf8\n    nullable: false\n",
        )
        .unwrap();
        graph
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(uuid::Uuid::from_u128(720)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: graphforge_core::OntologyMode::Strict,
            })
            .unwrap();
        let valid = graph
            .add_node(
                "Host",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Gateway".into())),
                    ("hostname".into(), PropValue::Str("gw-1".into())),
                ]),
            )
            .unwrap();
        let generation_uuid = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let topology_generation = graphforge_storage::read_topology_generation(&graph.dir).unwrap();

        let error = graph
            .add_node(
                "Host",
                &HashMap::from([("unknown_field".into(), PropValue::Str("must fail".into()))]),
            )
            .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            generation_uuid
        );
        assert_eq!(
            graphforge_storage::read_topology_generation(&graph.dir).unwrap(),
            topology_generation
        );
        let rows = graphforge_storage::read_node_property_rows(&graph.dir, "Host").unwrap();
        assert_eq!(rows.len(), 1);
        let properties = rows.get(valid.uuid.as_bytes()).unwrap();
        assert_eq!(
            properties.get("name"),
            Some(&IrLiteral::Str("Gateway".into()))
        );
        assert_eq!(
            properties.get("hostname"),
            Some(&IrLiteral::Str("gw-1".into()))
        );
    }

    #[test]
    fn undeclared_add_node_properties_remain_permitted_outside_strict_mode() {
        let exploratory = GraphForge::new(None).unwrap();
        exploratory
            .add_node(
                "Host",
                &HashMap::from([("unknown_field".into(), PropValue::Int(1))]),
            )
            .unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let ontology_path = dir.path().join("advisory.yaml");
        let mut advisory = GraphForge::new(dir.path().to_str()).unwrap();
        std::fs::write(
            &ontology_path,
            "ontology_id: construction\nversion: \"1\"\nentity_types:\n  - name: Host\n    abstract: false\n",
        )
        .unwrap();
        advisory
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(uuid::Uuid::from_u128(721)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: graphforge_core::OntologyMode::Advisory,
            })
            .unwrap();
        advisory
            .add_node(
                "Host",
                &HashMap::from([("unknown_field".into(), PropValue::Int(2))]),
            )
            .unwrap();
        assert_eq!(
            advisory
                .execute("MATCH (n:Host) RETURN n.unknown_field AS value")
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }
}
