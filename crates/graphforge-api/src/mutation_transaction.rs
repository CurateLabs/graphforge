//! Facade generation adapter for the execution-owned mutation transaction.
use std::sync::Arc;

use graphforge_core::GfError;
use graphforge_exec::mutation::MutationLifecycle;
use graphforge_ir::RuntimeCatalog;
use graphforge_storage::{ResolvedProjectGeneration, SemanticStorageBindings};

use crate::GraphForge;

pub(crate) struct FacadeMutationLifecycle<'a> {
    graph: &'a GraphForge,
    parent: ResolvedProjectGeneration,
    prior_catalog: RuntimeCatalog,
    publish: bool,
    bindings: Option<&'a SemanticStorageBindings>,
    unpublished: Option<graphforge_storage::GraphWorkspaceCheckpoint>,
}

impl<'a> FacadeMutationLifecycle<'a> {
    pub(crate) fn new(
        graph: &'a GraphForge,
        prior_catalog: RuntimeCatalog,
        publish: bool,
        bindings: Option<&'a SemanticStorageBindings>,
    ) -> Result<Self, GfError> {
        if graph.read_only {
            return Err(GfError::Execution(
                "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
            ));
        }
        let parent = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )?;
        let unpublished = if publish {
            None
        } else {
            Some(graphforge_storage::GraphWorkspaceCheckpoint::capture(
                &graph.dir,
            )?)
        };
        Ok(Self {
            graph,
            parent,
            prior_catalog,
            publish,
            bindings,
            unpublished,
        })
    }

    fn refresh_unpublished(&self) -> Result<(), GfError> {
        let (inventory, _) = graphforge_storage::capture_graph_files(&self.graph.dir)?;
        let inventory = Arc::new(
            graphforge_storage::AuthenticatedPropertyInventory::from_materialized_generation(
                &self.parent,
                &self.graph.dir,
                inventory.files,
            )?,
        );
        *self
            .graph
            .property_authority
            .lock()
            .expect("property authority lock poisoned") = crate::GenerationPropertyAuthority {
            generation_uuid: self.parent.generation_uuid(),
            inventory,
        };
        Ok(())
    }
}

impl MutationLifecycle for FacadeMutationLifecycle<'_> {
    fn publish(
        &mut self,
        outcome: &graphforge_exec::mutation::MutationOutcome,
        catalog: &RuntimeCatalog,
    ) -> Result<(), GfError> {
        *self
            .graph
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = catalog.clone();
        #[cfg(test)]
        {
            *self
                .graph
                .last_mutation_outcome
                .lock()
                .expect("mutation test outcome poisoned") = Some(outcome.clone());
        }
        if self.publish && !outcome.receipt.is_empty() {
            self.graph
                .publish_graph_mutation_with_bindings(&outcome.receipt, self.bindings)?;
        }
        Ok(())
    }

    fn complete(&mut self) -> Result<(), GfError> {
        if !self.publish {
            self.refresh_unpublished()?;
        }
        self.graph.adjacency_provider.invalidate();
        Ok(())
    }

    fn abort(&mut self) -> Result<(), GfError> {
        let health = self.graph.graph_visibility.health.clone();
        health.recover(|| self.restore())
    }
}

impl FacadeMutationLifecycle<'_> {
    fn restore(&mut self) -> Result<(), GfError> {
        // Resolve actual durable authority, not a possibly stale in-memory UUID.
        // An unknown CURRENT fails before any parent restoration.
        let current = graphforge_storage::resolve_project_generation(
            self.graph.resolved_generation.container_root(),
        )?;
        if current.generation_uuid() == self.parent.generation_uuid() {
            if let Some(checkpoint) = &mut self.unpublished {
                checkpoint.restore(&self.graph.dir)?;
                self.refresh_unpublished()?;
            } else {
                crate::rematerialize_graph_workspace(&self.parent, &self.graph.dir)?;
                self.graph.install_property_generation(&self.parent)?;
            }
            *self
                .graph
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned") = self.prior_catalog.clone();
        } else {
            // Publication may have succeeded before a later operation failed.
            // Reconcile from the selected generation; never reinstall old files.
            crate::rematerialize_graph_workspace(&current, &self.graph.dir)?;
            let catalog = crate::load_runtime_catalog(&self.graph.dir)?;
            self.graph.install_property_generation(&current)?;
            *self
                .graph
                .semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned") =
                graphforge_storage::semantic_storage_bindings(&current)?;
            *self
                .graph
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned") = catalog;
        }
        *self
            .graph
            .uuid_membership_index
            .lock()
            .expect("UUID membership index lock poisoned") = None;
        self.graph.adjacency_provider.invalidate();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Float64Array, Int64Array};
    use graphforge_core::algorithms::{ClusterAlgorithm, RankAlgorithm};
    use graphforge_core::{ClusterOptions, RankOptions};
    use std::collections::HashMap;

    fn algorithm(
        graph: &GraphForge,
        cluster: bool,
        property: Option<&str>,
    ) -> arrow::record_batch::RecordBatch {
        if cluster {
            graph
                .cluster(
                    "Person",
                    ClusterOptions {
                        by: ClusterAlgorithm::Components,
                        via: Some("KNOWS".into()),
                        directed: true,
                        write_property: property.map(str::to_owned),
                        ..Default::default()
                    },
                )
                .unwrap()
        } else {
            graph
                .rank(
                    "Person",
                    RankOptions {
                        by: RankAlgorithm::Degree,
                        via: Some("KNOWS".into()),
                        directed: true,
                        write_property: property.map(str::to_owned),
                    },
                )
                .unwrap()
        }
    }

    #[test]
    fn mixed_property_and_statement_preparation_rejects_second_without_losing_first() {
        use graphforge_exec::mutation::MutationTransaction;
        use graphforge_ir::{Binder, IrLiteral};
        for property_first in [false, true] {
            let graph = GraphForge::new(None).unwrap();
            let node = graph
                .add_node(
                    "Person",
                    &HashMap::from([("name".into(), crate::PropValue::Str("retained".into()))]),
                )
                .unwrap();
            let prior = graph.runtime_catalog.lock().unwrap().clone();
            let mut transaction = MutationTransaction::new(&prior);
            transaction.intern_property("metric", None).unwrap();
            let working = transaction.catalog();
            let plan = Binder::new(None, working.clone(), graph.ontology_mode)
                .bind(&graphforge_cypher::parse("MATCH (n:Person) SET n.metric=2").unwrap())
                .unwrap();
            let inventory = graph.property_inventory_for_session();
            let catalog =
                graphforge_storage::GraphCatalog::open_authenticated_with_semantic_bindings(
                    &graph.dir,
                    None,
                    &working.lock().unwrap(),
                    None,
                    inventory.clone(),
                )
                .unwrap();
            let session = graphforge_exec::ExecutionSession::new_with_target(
                catalog,
                None,
                graph.dir.clone(),
                graph.ontology_mode,
            )
            .unwrap();
            let resource = session.write_resource().unwrap();
            let (_, stem) = graph.algorithm_label("Person", "rank").unwrap();
            let updates = HashMap::from([(
                *node.uuid.as_bytes(),
                HashMap::from([("metric".into(), IrLiteral::Int(1))]),
            )]);
            let runtime = tokio::runtime::Runtime::new().unwrap();
            if property_first {
                transaction
                    .stage_node_properties(&resource, &inventory, &stem, &updates)
                    .unwrap();
            } else {
                runtime
                    .block_on(session.prepare_write_statement_with_params(
                        &plan,
                        &Default::default(),
                        &mut transaction,
                    ))
                    .unwrap();
            }
            let outcome = transaction.outcome();
            let error = if property_first {
                runtime
                    .block_on(session.prepare_write_statement_with_params(
                        &plan,
                        &Default::default(),
                        &mut transaction,
                    ))
                    .unwrap_err()
            } else {
                transaction
                    .stage_node_properties(&resource, &inventory, &stem, &updates)
                    .unwrap_err()
            };
            assert!(error.to_string().contains("already prepared"), "{error}");
            assert_eq!(transaction.outcome(), outcome);
            let mut lifecycle = FacadeMutationLifecycle::new(&graph, prior, true, None).unwrap();
            transaction.commit(&resource, true, &mut lifecycle).unwrap();
            assert_eq!(
                graph.last_mutation_outcome.lock().unwrap().as_ref(),
                Some(&outcome)
            );
            let result = graph.execute("MATCH (n:Person) RETURN n.metric").unwrap();
            let values = result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.values(), &[if property_first { 1 } else { 2 }]);
        }
    }

    #[test]
    fn fresh_and_populated_ephemeral_abort_restore_catalog_data_and_allow_new_writes() {
        for populated in [false, true] {
            let graph = GraphForge::new(None).unwrap();
            if populated {
                graph.execute("CREATE (:Person {name:'retained'})").unwrap();
            }
            let prior_catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
            let prior_files = graphforge_storage::capture_graph_files(&graph.dir)
                .unwrap()
                .0;
            let prior_generation = *graph.current_generation_uuid.lock().unwrap();
            let error = graph
                .execute(
                    "CREATE (a:Temporary {ephemeral_metric:1}) DELETE a SET a.ephemeral_metric=2",
                )
                .unwrap_err();
            assert!(!error.to_string().is_empty());
            assert_eq!(
                graph.runtime_catalog.lock().unwrap().to_record_batch(),
                prior_catalog
            );
            assert_eq!(
                graphforge_storage::capture_graph_files(&graph.dir)
                    .unwrap()
                    .0,
                prior_files
            );
            assert_eq!(
                *graph.current_generation_uuid.lock().unwrap(),
                prior_generation
            );
            let result = graph.execute("MATCH (n) RETURN n").unwrap();
            assert_eq!(
                result
                    .batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>(),
                usize::from(populated)
            );
            graph
                .execute("CREATE (:Person {name:'after_abort'})")
                .unwrap();
            assert_eq!(
                graph.node_count("Person").unwrap(),
                if populated { 2 } else { 1 }
            );
        }
    }

    #[test]
    fn real_analyst_and_cypher_share_receipts_counters_values_and_reopen() {
        for cluster in [false, true] {
            for analyst_first in [false, true] {
                let root = tempfile::TempDir::new().unwrap();
                let graph = GraphForge::new(root.path().to_str()).unwrap();
                let mut names = HashMap::new();
                let mut nodes = Vec::new();
                for name in ["a", "b", "c"] {
                    let node = graph
                        .add_node(
                            "Person",
                            &HashMap::from([("name".into(), crate::PropValue::Str(name.into()))]),
                        )
                        .unwrap();
                    names.insert(*node.uuid.as_bytes(), name);
                    nodes.push(node);
                }
                graph
                    .execute(if cluster {
                        "MATCH (n:Person) SET n.metric = 0"
                    } else {
                        "MATCH (n:Person) SET n.metric = 0.0"
                    })
                    .unwrap();
                graph
                    .add_edge(&nodes[0], "KNOWS", &nodes[1], &HashMap::new())
                    .unwrap();
                // Warm actual property and adjacency resources before either write.
                graph
                    .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
                    .unwrap();
                let dry = algorithm(&graph, cluster, None);
                let uuids = dry
                    .column_by_name("node_uuid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let mut cases = Vec::new();
                for row in 0..dry.num_rows() {
                    let uuid: [u8; 16] = uuids.value(row).try_into().unwrap();
                    let value = if cluster {
                        dry.column_by_name("community_id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row)
                            .to_string()
                    } else {
                        format!(
                            "{:?}",
                            dry.column_by_name("score")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row)
                        )
                    };
                    cases.push(format!("WHEN '{}' THEN {value}", names[&uuid]));
                }
                let query = format!(
                    "MATCH (n:Person) SET n.metric = CASE n.name {} END",
                    cases.join(" ")
                );
                let topology = graphforge_storage::read_topology_generation(&graph.dir).unwrap();
                let before = *graph.current_generation_uuid.lock().unwrap();
                let mut outcomes = Vec::new();
                for analyst in [analyst_first, !analyst_first] {
                    if analyst {
                        algorithm(&graph, cluster, Some("metric"));
                    } else {
                        graph.execute(&query).unwrap();
                    }
                    outcomes.push(graph.last_mutation_outcome.lock().unwrap().clone().unwrap());
                    assert_eq!(
                        graphforge_storage::read_topology_generation(&graph.dir).unwrap(),
                        topology
                    );
                    assert_ne!(*graph.current_generation_uuid.lock().unwrap(), before);
                    assert!(
                        graph.runtime_catalog.lock().unwrap().contains_property(
                            "metric",
                            if analyst { Some("Person") } else { None }
                        )
                    );
                }
                // Same identities are deliberately reused, so every receipt field
                // and its deterministic subject ordering can be compared directly.
                assert_eq!(outcomes[0], outcomes[1]);
                assert_eq!(outcomes[0].side_effects.properties_set, 3);
                assert_eq!(outcomes[0].side_effects.properties_removed, 3);
                graph
                    .execute(&query.replace("n.metric", "n.fresh_cypher"))
                    .unwrap();
                let fresh_cypher = graph.last_mutation_outcome.lock().unwrap().clone().unwrap();
                algorithm(&graph, cluster, Some("fresh_analyst"));
                let fresh_analyst = graph.last_mutation_outcome.lock().unwrap().clone().unwrap();
                assert_eq!(fresh_cypher, fresh_analyst);
                assert_eq!(fresh_analyst.side_effects.properties_set, 3);
                assert_eq!(fresh_analyst.side_effects.properties_removed, 0);
                let catalog = graph.runtime_catalog.lock().unwrap();
                assert!(catalog.contains_property("fresh_cypher", None));
                assert!(catalog.contains_property("fresh_analyst", Some("Person")));
                drop(catalog);
                let values = graph.execute("MATCH (n:Person) RETURN n.fresh_cypher AS expected, n.fresh_analyst AS actual ORDER BY n.name").unwrap();
                for batch in &values.batches {
                    assert_eq!(batch.column(0), batch.column(1));
                }

                assert_eq!(outcomes[0].receipt.effects.len(), 1);
                assert_eq!(outcomes[0].receipt.effects[0].outputs.len(), 3);
                let values = graph
                    .execute("MATCH (n:Person) RETURN n.name, n.metric ORDER BY n.name")
                    .unwrap()
                    .batches;
                drop(graph);
                let reopened = GraphForge::new(root.path().to_str()).unwrap();
                let actual = reopened
                    .execute("MATCH (n:Person) RETURN n.name, n.metric ORDER BY n.name")
                    .unwrap()
                    .batches;
                assert_eq!(actual.len(), values.len());
                for (actual, expected) in actual.iter().zip(&values) {
                    assert_eq!(actual.schema().fields(), expected.schema().fields());
                    let mut actual_metadata = actual.schema().metadata().clone();
                    let mut expected_metadata = expected.schema().metadata().clone();
                    let actual_query = actual_metadata.remove("graphforge.query_id").unwrap();
                    let expected_query = expected_metadata.remove("graphforge.query_id").unwrap();
                    assert!(uuid::Uuid::parse_str(&actual_query).is_ok());
                    assert!(uuid::Uuid::parse_str(&expected_query).is_ok());
                    assert_ne!(actual_query, expected_query);
                    assert_eq!(actual_metadata, expected_metadata);
                    assert_eq!(actual.columns(), expected.columns());
                }
                assert!(
                    reopened
                        .runtime_catalog
                        .lock()
                        .unwrap()
                        .contains_property("metric", Some("Person"))
                );
            }
        }
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::process::Command;

    #[test]
    fn failed_restore_helper() {
        let Ok(root) = std::env::var("GF_MUTATION_RESTORE_ROOT") else {
            return;
        };
        if std::env::var("GF_MUTATION_STANDALONE").as_deref() == Ok("1") {
            failed_standalone_restore(&root);
            return;
        }
        let graph = GraphForge::new(Some(&root)).unwrap();
        let (mut stream, _, _retained) = graph
            .execute_stream_owned("MATCH (n:Person) RETURN n.name", &HashMap::new())
            .unwrap();
        let permit = graph.graph_visibility.lock().unwrap();
        let error = graph
            .execute_write_without_publish(
                "MATCH (n:Person) DELETE n SET n.metric = 1",
                &HashMap::new(),
            )
            .unwrap_err();
        drop(permit);
        let message = error.to_string();
        assert!(message.contains("mutation restore failed"), "{message}");
        let backup = message
            .split("rollback backup retained at ")
            .nth(1)
            .unwrap()
            .split(": ")
            .next()
            .unwrap();
        assert!(
            std::path::Path::new(backup)
                .join("topology/nodes.parquet")
                .exists(),
            "{message}"
        );
        for query in ["MATCH (n:Person) RETURN n", "CREATE (:Person)"] {
            assert!(
                graph
                    .execute(query)
                    .unwrap_err()
                    .to_string()
                    .contains("unavailable after failed recovery")
            );
        }
        assert!(graph.labels().is_err());
        assert!(graph.node_count("Person").is_err());
        assert!(graph.rank("Person", Default::default()).is_err());
        assert!(graph.cluster("Person", Default::default()).is_err());
        let output = std::path::Path::new(&root).join("must-not-export.gf");
        assert!(
            graph
                .export_portable(crate::PortableExportRequest {
                    selection: crate::PortableSelection::Current,
                    output: output.clone()
                })
                .is_err()
        );
        assert!(!output.exists());
        graph
            .block_on(async {
                let error = stream
                    .next()
                    .await
                    .expect("retained stream must report failure")
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("unavailable after failed recovery")
                );
                assert!(stream.next().await.is_none());
                Ok(())
            })
            .unwrap();
        // The durable parent is still authoritative and usable by a fresh owner.
        let reopened = GraphForge::new(Some(&root)).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), 2);
        std::fs::remove_dir_all(backup).unwrap();
    }

    #[test]
    fn failed_restore_retains_backup_and_denies_later_reads_writes_and_lazy_stream() {
        let root = tempfile::TempDir::new().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})")
            .unwrap();
        drop(graph);
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("mutation_transaction::recovery_tests::failed_restore_helper")
            .arg("--nocapture")
            .env("GF_MUTATION_RESTORE_ROOT", root.path())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "mutation.restore.after_copy.error",
            )
            .status()
            .unwrap();
        assert!(status.success(), "restore failure helper: {status}");
    }
    fn failed_standalone_restore(root: &str) {
        let graph = GraphForge::new(Some(root)).unwrap();
        let catalog = graph.runtime_catalog();
        let bind = |query: &str| {
            graphforge_ir::Binder::new(
                None,
                Arc::clone(&catalog),
                graphforge_core::OntologyMode::Exploratory,
            )
            .bind(&graphforge_cypher::parse(query).unwrap())
            .unwrap()
        };
        let read = bind("MATCH (n:Person) RETURN n.name");
        let write = bind("MATCH (n:Person) DELETE n SET n.metric = 1");
        let physical_catalog =
            graphforge_storage::GraphCatalog::open(&graph.dir, None, &catalog.lock().unwrap())
                .unwrap();
        let session = graphforge_exec::ExecutionSession::new_with_target(
            physical_catalog,
            None,
            graph.dir.clone(),
            graphforge_core::OntologyMode::Exploratory,
        )
        .unwrap();
        let mut stream = graph
            .block_on(session.execute_plan_stream(&read, &HashMap::new()))
            .unwrap();
        let error = graph
            .block_on(session.execute_write_statement(&write))
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("mutation restore failed"), "{message}");
        let backup = message
            .split("rollback backup retained at ")
            .nth(1)
            .unwrap()
            .split(": ")
            .next()
            .unwrap();
        assert!(
            std::path::Path::new(backup)
                .join("topology/nodes.parquet")
                .exists()
        );
        assert!(
            graph
                .block_on(session.execute_plan(&read))
                .unwrap_err()
                .to_string()
                .contains("unavailable after failed recovery")
        );
        assert!(
            graph
                .block_on(session.execute_write_statement(&write))
                .unwrap_err()
                .to_string()
                .contains("unavailable after failed recovery")
        );
        assert!(session.write_resource().is_err());
        graph
            .block_on(async {
                assert!(
                    stream
                        .next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("unavailable after failed recovery")
                );
                assert!(stream.next().await.is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            GraphForge::new(Some(root))
                .unwrap()
                .node_count("Person")
                .unwrap(),
            2
        );
        std::fs::remove_dir_all(backup).unwrap();
    }

    #[test]
    fn standalone_failed_restore_denies_retained_stream_and_same_session_writes() {
        let root = tempfile::TempDir::new().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})")
            .unwrap();
        drop(graph);
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("mutation_transaction::recovery_tests::failed_restore_helper")
            .arg("--nocapture")
            .env("GF_MUTATION_RESTORE_ROOT", root.path())
            .env("GF_MUTATION_STANDALONE", "1")
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "mutation.restore.after_copy.error",
            )
            .status()
            .unwrap();
        assert!(
            status.success(),
            "standalone restore failure helper: {status}"
        );
    }
}
