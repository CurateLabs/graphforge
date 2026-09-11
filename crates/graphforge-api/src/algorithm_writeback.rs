//! Transactional opt-in property write-back for Rust analyst algorithms.
#![allow(
    dead_code,
    reason = "wired by the ordered algorithm vertical algorithm slices"
)]

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::Algorithm;
use graphforge_ir::IrLiteral;

use super::{GfError, GraphForge};

impl GraphForge {
    /// Atomically persist one rank/cluster result column when explicitly requested.
    pub(crate) fn write_algorithm_property(
        &self,
        label: &str,
        stem: &str,
        algorithm: Algorithm,
        property: Option<&str>,
        batch: &RecordBatch,
    ) -> Result<u64, GfError> {
        let Some(property) = property else {
            return Ok(0);
        };
        validate_property_name(property)?;
        let (value_name, expected) = match algorithm {
            Algorithm::Rank(_) => ("score", DataType::Float64),
            Algorithm::Cluster(_) => ("community_id", DataType::Int64),
            _ => {
                return Err(GfError::Validation(
                    "write_property is available only for rank and cluster".into(),
                ));
            }
        };
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let uuids = fixed_uuid_column(batch, "node_uuid")?;
        let values = batch.column_by_name(value_name).ok_or_else(|| {
            GfError::Validation(format!("algorithm result is missing {value_name:?}"))
        })?;
        if values.data_type() != &expected || values.null_count() > 0 {
            return Err(GfError::Validation(format!(
                "algorithm result {value_name:?} must be non-null {expected:?}"
            )));
        }
        let workspace = self.workspace_for_session();
        let dir = workspace.path().to_path_buf();
        reject_property_collision(
            &dir,
            &self.property_inventory_for_session(),
            stem,
            property,
            &expected,
        )?;

        let known = persisted_node_uuids(&dir)?;
        let updates = algorithm_property_updates(
            algorithm,
            property,
            batch.num_rows(),
            uuids,
            values,
            &known,
        )?;

        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut transaction = graphforge_exec::mutation::MutationTransaction::new(&prior_catalog);
        transaction.intern_property(property, Some(label))?;
        let working = transaction.catalog();
        let catalog = graphforge_storage::GraphCatalog::open_authenticated_with_semantic_bindings(
            &dir,
            self.ontology.as_ref(),
            &working.lock().expect("mutation catalog poisoned"),
            self.semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .as_ref(),
            self.property_inventory_for_session(),
        )
        .map_err(GfError::from_execution_error)?;
        let session =
            graphforge_exec::ExecutionSession::new_with_target_provider_resources_and_identity(
                catalog,
                self.ontology.clone(),
                dir,
                self.ontology_mode,
                self.adjacency_provider_for_session(),
                Some(std::sync::Arc::clone(&self.ordinal_identities)),
                &self.session_resource_config(),
            )?;
        let session = if self.read_only {
            session.restrict_to_reads()
        } else {
            session
        };
        let resource = session.write_resource()?;
        let mut lifecycle = crate::mutation_transaction::FacadeMutationLifecycle::new(
            self,
            prior_catalog,
            true,
            None,
        )?;
        let touched = match transaction.stage_node_properties(
            &resource,
            &self.property_inventory_for_session(),
            stem,
            &updates,
        ) {
            Ok(touched) => touched,
            Err(error) => return transaction.abort(&mut lifecycle, error),
        };
        transaction.commit(&resource, true, &mut lifecycle)?;
        Ok(touched)
    }
}

fn algorithm_property_updates(
    algorithm: Algorithm,
    property: &str,
    row_count: usize,
    uuids: &FixedSizeBinaryArray,
    values: &arrow::array::ArrayRef,
    known: &HashSet<[u8; 16]>,
) -> Result<HashMap<[u8; 16], HashMap<String, IrLiteral>>, GfError> {
    let mut seen = HashSet::with_capacity(row_count);
    let mut updates = HashMap::with_capacity(row_count);
    for row in 0..row_count {
        let uuid = uuid_at(uuids, row)?;
        if !seen.insert(uuid) {
            return Err(GfError::Validation(
                "algorithm result contains duplicate node_uuid rows".into(),
            ));
        }
        if !known.contains(&uuid) {
            return Err(GfError::Validation(
                "algorithm result contains a node_uuid outside this graph".into(),
            ));
        }
        let value = match algorithm {
            Algorithm::Rank(_) => {
                let value = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("type checked")
                    .value(row);
                if !value.is_finite() {
                    return Err(GfError::Validation(
                        "rank write_property values must be finite".into(),
                    ));
                }
                IrLiteral::Float(value)
            }
            Algorithm::Cluster(_) => IrLiteral::Int(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("type checked")
                    .value(row),
            ),
            _ => unreachable!("verb checked by caller"),
        };
        updates.insert(uuid, HashMap::from([(property.to_owned(), value)]));
    }
    Ok(updates)
}

fn validate_property_name(name: &str) -> Result<(), GfError> {
    const RESERVED: [&str; 10] = [
        "node_uuid",
        "node_id",
        "edge_uuid",
        "edge_id",
        "src_uuid",
        "src_id",
        "dst_uuid",
        "dst_id",
        "score",
        "community_id",
    ];
    if name.is_empty()
        || name.trim() != name
        || name.chars().any(char::is_control)
        || RESERVED.contains(&name)
    {
        return Err(GfError::Validation(format!(
            "invalid algorithm write_property name {name:?}"
        )));
    }
    Ok(())
}

fn fixed_uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16 && column.null_count() == 0)
        .ok_or_else(|| {
            GfError::Validation(format!(
                "algorithm result {name:?} must be non-null FixedSizeBinary(16)"
            ))
        })
}

fn uuid_at(column: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    column
        .value(row)
        .try_into()
        .map_err(|_| GfError::Validation("malformed algorithm node_uuid".into()))
}

fn persisted_node_uuids(dir: &std::path::Path) -> Result<HashSet<[u8; 16]>, GfError> {
    let mut known = HashSet::new();
    for batch in
        graphforge_storage::read_nodes(dir).map_err(|error| GfError::Storage(error.to_string()))?
    {
        let uuids = fixed_uuid_column(&batch, "node_uuid")?;
        for row in 0..uuids.len() {
            known.insert(uuid_at(uuids, row)?);
        }
    }
    Ok(known)
}

fn reject_property_collision(
    dir: &std::path::Path,
    inventory: &graphforge_storage::AuthenticatedPropertyInventory,
    stem: &str,
    property: &str,
    expected: &DataType,
) -> Result<(), GfError> {
    let batches = graphforge_storage::read_properties_from_inventory(dir, inventory, stem)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    for batch in batches {
        if let Ok(field) = batch.schema().field_with_name(property)
            && field.data_type() != expected
        {
            return Err(GfError::Validation(format!(
                "write_property {property:?} collides with existing {:?} data",
                field.data_type()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use graphforge_core::algorithms::{ClusterAlgorithm, RankAlgorithm};

    use super::*;

    fn graph() -> (tempfile::TempDir, GraphForge, [u8; 16]) {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        graph.execute("CREATE (:Person)").unwrap();
        let nodes = graphforge_storage::read_nodes(&graph.dir()).unwrap();
        let uuids = fixed_uuid_column(&nodes[0], "node_uuid").unwrap();
        let uuid = uuid_at(uuids, 0).unwrap();
        (dir, graph, uuid)
    }

    fn result(uuids: &[[u8; 16]], name: &str, values: Arc<dyn Array>) -> RecordBatch {
        let uuids =
            FixedSizeBinaryArray::try_from_iter(uuids.iter().map(<[u8; 16]>::as_slice)).unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(name, values.data_type().clone(), false),
            ])),
            vec![Arc::new(uuids), values],
        )
        .unwrap()
    }

    fn authenticated_properties(
        graph: &GraphForge,
        uuid: [u8; 16],
    ) -> graphforge_storage::PropertySnapshotRow {
        let (mut rows, _) = graphforge_storage::read_authenticated_property_snapshots_for(
            &graph.dir(),
            graphforge_storage::PropertyRouteKind::Node,
            "_untyped",
            &BTreeSet::from([uuid]),
        )
        .unwrap();
        rows.remove(&uuid).expect("authenticated property row")
    }

    fn write(
        graph: &GraphForge,
        algorithm: Algorithm,
        property: Option<&str>,
        batch: &RecordBatch,
    ) -> Result<u64, GfError> {
        graph.write_algorithm_property("Person", "_untyped", algorithm, property, batch)
    }

    #[test]
    fn existing_cypher_property_counter_semantics() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name:'seed'})").unwrap();
        let repeated = graph
            .execute("MATCH (n:Person) SET n.metric=1 SET n.metric=2")
            .unwrap();
        assert_eq!(repeated.side_effects.unwrap().properties_set, 1);
        let same_value = graph.execute("MATCH (n:Person) SET n.metric=2").unwrap();
        assert_eq!(same_value.side_effects.unwrap().properties_set, 1);
        let created = graph
            .execute("CREATE (n:NewPerson {metric:1}) SET n.metric=2")
            .unwrap();
        assert_eq!(created.side_effects.unwrap().properties_set, 1);
        let empty = graph.execute("MATCH (n:Absent) SET n.metric=3").unwrap();
        assert_eq!(empty.side_effects.unwrap().properties_set, 0);
    }

    #[test]
    fn rank_and_cluster_writes_persist_without_topology_changes() {
        let (dir, graph, uuid) = graph();
        let generation = graphforge_storage::read_topology_generation(&graph.dir()).unwrap();
        let rank = result(&[uuid], "score", Arc::new(Float64Array::from(vec![0.75])));
        let cluster = result(&[uuid], "community_id", Arc::new(Int64Array::from(vec![7])));
        let rank_algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let cluster_algorithm = Algorithm::Cluster(ClusterAlgorithm::Components);
        assert_eq!(write(&graph, rank_algorithm, None, &rank).unwrap(), 0);
        let empty = RecordBatch::new_empty(rank.schema());
        assert_eq!(
            write(&graph, rank_algorithm, Some("ranked"), &empty).unwrap(),
            0
        );
        assert!(
            graphforge_storage::enumerate_property_fragments(
                &graph.dir(),
                graphforge_storage::PropertyRouteKind::Node,
                "_untyped",
            )
            .unwrap()
            .is_empty()
        );
        write(&graph, rank_algorithm, Some("ranked"), &rank).unwrap();
        write(&graph, cluster_algorithm, Some("group"), &cluster).unwrap();
        assert_eq!(
            graphforge_storage::read_topology_generation(&graph.dir()).unwrap(),
            generation
        );
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let props =
            graphforge_storage::read_entity_properties(&reopened.dir(), "_untyped", &uuid, false)
                .unwrap();
        assert_eq!(props["ranked"], IrLiteral::Float(0.75));
        assert_eq!(props["group"], IrLiteral::Int(7));
        let catalog = reopened.runtime_catalog.lock().unwrap();
        assert!(catalog.properties_for("Person").contains(&"ranked"));
        assert!(catalog.properties_for("Person").contains(&"group"));
    }

    #[test]
    fn validation_failures_leave_existing_properties_unchanged() {
        let (_dir, graph, uuid) = graph();
        graph
            .execute("MATCH (n:Person) SET n.metric = 'old'")
            .unwrap();
        let before = authenticated_properties(&graph, uuid);
        assert_eq!(before.values["metric"], IrLiteral::Str("old".into()));
        let fragments_before = graphforge_storage::enumerate_property_fragments(
            &graph.dir(),
            graphforge_storage::PropertyRouteKind::Node,
            "_untyped",
        )
        .unwrap();
        let rank = result(&[uuid], "score", Arc::new(Float64Array::from(vec![1.0])));
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        assert!(write(&graph, algorithm, Some("metric"), &rank).is_err());
        let wrong = result(&[uuid], "score", Arc::new(StringArray::from(vec!["bad"])));
        assert!(write(&graph, algorithm, Some("ranked"), &wrong).is_err());
        let partial = result(
            &[uuid, [9; 16]],
            "score",
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        );
        assert!(write(&graph, algorithm, Some("ranked"), &partial).is_err());
        assert!(write(&graph, algorithm, Some(""), &rank).is_err());
        assert_eq!(authenticated_properties(&graph, uuid), before);
        assert_eq!(
            graphforge_storage::enumerate_property_fragments(
                &graph.dir(),
                graphforge_storage::PropertyRouteKind::Node,
                "_untyped",
            )
            .unwrap(),
            fragments_before,
            "validation failures must not publish property authority"
        );
    }
}
