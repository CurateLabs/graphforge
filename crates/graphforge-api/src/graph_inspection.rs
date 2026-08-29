//! Deterministic graph inspection over one committed project generation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, ListArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use graphforge_core::{ApiErrorCode, GfError};

use crate::GraphForge;

const NODE_LABELS_QUERY: &str = "MATCH (n) RETURN labels(n) AS labels";
const RELATIONSHIP_TYPES_QUERY: &str = "MATCH ()-[r]->() RETURN type(r) AS relationship_type";

#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct GraphInspection {
    total_nodes: u64,
    label_counts: BTreeMap<String, u64>,
    relationship_type_counts: BTreeMap<String, u64>,
}

impl GraphInspection {
    pub(crate) fn labels(&self) -> Vec<String> {
        self.label_counts.keys().cloned().collect()
    }

    pub(crate) fn relationship_types(&self) -> Vec<String> {
        self.relationship_type_counts.keys().cloned().collect()
    }

    pub(crate) fn node_count(&self, label: &str) -> u64 {
        if label.is_empty() {
            self.total_nodes
        } else {
            self.label_counts.get(label).copied().unwrap_or(0)
        }
    }

    pub(crate) fn edge_count(&self) -> Result<u64, GfError> {
        let mut total = 0_u64;
        for count in self.relationship_type_counts.values() {
            total = total
                .checked_add(*count)
                .ok_or_else(|| resource_limit("graph inspection edge count exceeds UInt64"))?;
        }
        Ok(total)
    }

    pub(crate) fn into_record_batch(self) -> Result<RecordBatch, GfError> {
        let row_count = self
            .label_counts
            .len()
            .checked_add(self.relationship_type_counts.len())
            .ok_or_else(|| resource_limit("graph inspection row count exceeds usize"))?;
        let mut labels = Vec::with_capacity(row_count);
        let mut node_counts = Vec::with_capacity(row_count);
        let mut relationship_types = Vec::with_capacity(row_count);
        let mut relationship_counts = Vec::with_capacity(row_count);

        for (label, count) in self.label_counts {
            labels.push(Some(label));
            node_counts.push(Some(count));
            relationship_types.push(None);
            relationship_counts.push(None);
        }
        for (relationship_type, count) in self.relationship_type_counts {
            labels.push(None);
            node_counts.push(None);
            relationship_types.push(Some(relationship_type));
            relationship_counts.push(Some(count));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, true),
            Field::new("node_count", DataType::UInt64, true),
            Field::new("rel_type", DataType::Utf8, true),
            Field::new("rel_count", DataType::UInt64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(labels)) as ArrayRef,
                Arc::new(UInt64Array::from(node_counts)),
                Arc::new(StringArray::from(relationship_types)),
                Arc::new(UInt64Array::from(relationship_counts)),
            ],
        )
        .map_err(|error| schema_error(format!("build graph inspection schema: {error}")))
    }
}

impl GraphForge {
    /// Inspect one committed graph generation through the logical query path.
    ///
    /// The generation pin prevents concurrent publication from mixing node and
    /// relationship counts. The current runtime catalog is cloned into the
    /// private view because in-memory projects intentionally do not persist it;
    /// the catalog supplies names only, while logical rows determine presence.
    pub(crate) fn inspect_graph(&self) -> Result<GraphInspection, GfError> {
        let generation = self.generation_for_read()?;
        let mut view = Self::open_resolved_with_lifecycle_mode(
            generation.container_root().to_path_buf(),
            generation,
            true,
            self.lifecycle_mode,
        )?;
        let catalog = self
            .runtime_catalog
            .lock()
            .map_err(|_| GfError::Storage("runtime catalog lock poisoned".into()))?
            .clone();
        view.runtime_catalog = Arc::new(Mutex::new(catalog));

        let node_stream = view.execute_stream(NODE_LABELS_QUERY)?;
        let inspection = view.block_on(inspection_from_node_stream(node_stream))?;
        let relationship_stream = view.execute_stream(RELATIONSHIP_TYPES_QUERY)?;
        view.block_on(inspection_from_relationship_stream(
            relationship_stream,
            inspection,
        ))
    }
}

async fn inspection_from_node_stream(
    mut stream: graphforge_exec::SendableRecordBatchStream,
) -> Result<GraphInspection, GfError> {
    let mut inspection = GraphInspection::default();
    while let Some(batch) = stream.next().await {
        accumulate_node_batch(&mut inspection, &batch.map_err(execution_error)?)?;
    }
    Ok(inspection)
}

async fn inspection_from_relationship_stream(
    mut stream: graphforge_exec::SendableRecordBatchStream,
    mut inspection: GraphInspection,
) -> Result<GraphInspection, GfError> {
    while let Some(batch) = stream.next().await {
        accumulate_relationship_batch(&mut inspection, &batch.map_err(execution_error)?)?;
    }
    Ok(inspection)
}

fn accumulate_node_batch(
    inspection: &mut GraphInspection,
    batch: &RecordBatch,
) -> Result<(), GfError> {
    inspection.total_nodes = inspection
        .total_nodes
        .checked_add(
            u64::try_from(batch.num_rows())
                .map_err(|_| resource_limit("graph inspection node batch exceeds UInt64"))?,
        )
        .ok_or_else(|| resource_limit("graph inspection node count exceeds UInt64"))?;
    let labels = batch
        .column_by_name("labels")
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| schema_error("graph inspection labels column is not List<Utf8>"))?;
    for row in 0..batch.num_rows() {
        if labels.is_null(row) {
            return Err(schema_error("graph inspection labels row is null"));
        }
        let values = labels.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| schema_error("graph inspection label item is not Utf8"))?;
        let mut unique = BTreeSet::new();
        for index in 0..values.len() {
            if values.is_null(index) {
                return Err(schema_error("graph inspection label item is null"));
            }
            unique.insert(values.value(index));
        }
        for label in unique {
            increment(&mut inspection.label_counts, label)?;
        }
    }
    Ok(())
}

fn accumulate_relationship_batch(
    inspection: &mut GraphInspection,
    batch: &RecordBatch,
) -> Result<(), GfError> {
    let relationship_types = batch
        .column_by_name("relationship_type")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| schema_error("graph inspection relationship_type column is not Utf8"))?;
    for row in 0..batch.num_rows() {
        if relationship_types.is_null(row) {
            return Err(schema_error(
                "graph inspection relationship_type row is null",
            ));
        }
        increment(
            &mut inspection.relationship_type_counts,
            relationship_types.value(row),
        )?;
    }
    Ok(())
}

fn execution_error(error: datafusion::error::DataFusionError) -> GfError {
    GfError::Execution(error.to_string())
}

fn increment(counts: &mut BTreeMap<String, u64>, name: &str) -> Result<(), GfError> {
    let count = counts.entry(name.to_owned()).or_default();
    *count = count
        .checked_add(1)
        .ok_or_else(|| resource_limit("graph inspection count exceeds UInt64"))?;
    Ok(())
}

fn schema_error(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: message.into(),
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, UInt64Array};
    use graphforge_core::OntologyMode;
    use tempfile::TempDir;

    use super::*;
    use crate::{AdoptOntologyRequest, OperationId, WriteContext};

    fn assert_schema(batch: &RecordBatch) {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("label", &DataType::Utf8, true),
                ("node_count", &DataType::UInt64, true),
                ("rel_type", &DataType::Utf8, true),
                ("rel_count", &DataType::UInt64, true),
            ]
        );
    }

    #[test]
    fn empty_graph_inspection_is_exact() {
        let graph = GraphForge::new(None).unwrap();
        let inspection = graph.inspect_graph().unwrap();
        assert_eq!(inspection.labels(), Vec::<String>::new());
        assert_eq!(inspection.relationship_types(), Vec::<String>::new());
        assert_eq!(inspection.node_count(""), 0);
        assert_eq!(inspection.node_count("Missing"), 0);
        let batch = inspection.into_record_batch().unwrap();
        assert_schema(&batch);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn inspection_counts_multi_label_nodes_and_relationships_deterministically() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person:Author {name:'Alice'}), \
                 (b:Person {name:'Bob'}), (c:Paper {title:'Work'}), \
                 (a)-[:AUTHORED]->(c), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
            )
            .unwrap();

        let inspection = graph.inspect_graph().unwrap();
        assert_eq!(
            inspection.labels(),
            ["Author", "Paper", "Person"].map(str::to_owned)
        );
        assert_eq!(
            inspection.relationship_types(),
            ["AUTHORED", "KNOWS"].map(str::to_owned)
        );
        assert_eq!(inspection.node_count(""), 3);
        assert_eq!(inspection.node_count("Author"), 1);
        assert_eq!(inspection.node_count("Person"), 2);
        assert_eq!(inspection.node_count("Person') MATCH (n) RETURN n //"), 0);

        let batch = inspection.into_record_batch().unwrap();
        assert_schema(&batch);
        assert_eq!(batch.num_rows(), 5);
        let labels = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let node_counts = batch
            .column_by_name("node_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let relationship_types = batch
            .column_by_name("rel_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let relationship_counts = batch
            .column_by_name("rel_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            [Some("Author"), Some("Paper"), Some("Person"), None, None]
        );
        assert_eq!(
            node_counts.iter().collect::<Vec<_>>(),
            [Some(1), Some(1), Some(2), None, None]
        );
        assert_eq!(
            relationship_types.iter().collect::<Vec<_>>(),
            [None, None, None, Some("AUTHORED"), Some("KNOWS")]
        );
        assert_eq!(
            relationship_counts.iter().collect::<Vec<_>>(),
            [None, None, None, Some(1), Some(2)]
        );
    }

    #[test]
    fn persistent_inspection_reopens_with_the_same_values() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
            .unwrap();
        let before = graph.inspect_graph().unwrap();
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(reopened.inspect_graph().unwrap(), before);
    }

    #[test]
    fn strict_ontology_inspection_reports_runtime_names_that_are_present() {
        let directory = TempDir::new().unwrap();
        let imports = TempDir::new().unwrap();
        let ontology_path = imports.path().join("ontology.yaml");
        std::fs::write(
            &ontology_path,
            r#"ontology_id: inspection
version: "v1"
entity_types:
  - {name: Person, abstract: false}
  - {name: Unused, abstract: false}
relation_types:
  - {name: KNOWS, src: Person, dst: Person, semantic: {}}
  - {name: UNUSED_REL, src: Person, dst: Person, semantic: {}}
properties: []
constraints: []
migrations: []
"#,
        )
        .unwrap();

        let mut graph = GraphForge::new(directory.path().to_str()).unwrap();
        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(333)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        graph
            .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
            .unwrap();

        let inspection = graph.inspect_graph().unwrap();
        assert_eq!(inspection.labels(), ["Person".to_owned()]);
        assert_eq!(inspection.relationship_types(), ["KNOWS".to_owned()]);
        assert_eq!(inspection.node_count(""), 2);
        assert_eq!(inspection.node_count("Person"), 2);
        assert_eq!(inspection.node_count("Unused"), 0);
    }
}
