//! Bulk publication ownership.

use super::{
    Arc, BTreeSet, BulkEdgePublicationError, BulkEdgeRow, BulkInputKind, BulkNodePublicationError,
    BulkNodeRow, BulkValidationReason, DataType, Digest, Field, FixedSizeBinaryArray, GraphForge,
    HashMap, OperationId, RecordBatch, Schema, SchemaRef, Sha256, StringArray, UInt64Array, Uuid,
    ValidatedBulkNodes, contract_metadata, indexed_existing, open_membership_index,
    register_existing_endpoints, row_error,
};

fn bulk_node_generation_uuid(operation_uuid: OperationId, rows: &[BulkNodeRow]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-bulk-node-publication/1");
    hasher.update(operation_uuid.0.as_bytes());
    for row in rows {
        hasher.update(row.row_ordinal.to_le_bytes());
        hasher.update(row.node_uuid.as_bytes());
        hasher.update(row.label.as_bytes());
        hasher.update([0]);
        for (name, value) in &row.properties {
            hasher.update(name.as_bytes());
            hasher.update([0]);
            hasher.update(format!("{value:?}").as_bytes());
            hasher.update([0]);
        }
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn bulk_edge_generation_uuid(operation_uuid: OperationId, rows: &[BulkEdgeRow]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-bulk-edge-publication/1");
    hasher.update(operation_uuid.0.as_bytes());
    for row in rows {
        hasher.update(row.row_ordinal.to_le_bytes());
        hasher.update(row.edge_uuid.as_bytes());
        hasher.update(row.rel_type.as_bytes());
        hasher.update([0]);
        hasher.update(row.source_uuid.as_bytes());
        hasher.update(row.target_uuid.as_bytes());
        for (name, value) in &row.properties {
            hasher.update(name.as_bytes());
            hasher.update([0]);
            hasher.update(format!("{value:?}").as_bytes());
            hasher.update([0]);
        }
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn node_receipt(
    rows: &[BulkNodeRow],
    operation_uuid: OperationId,
    generation_uuid: Uuid,
) -> Result<RecordBatch, crate::GfError> {
    receipt(
        rows.iter().map(|row| {
            (
                row.row_ordinal,
                "node",
                row.node_uuid,
                Some(row.label.as_str()),
                None,
                None,
                None,
            )
        }),
        rows.len(),
        operation_uuid,
        generation_uuid,
    )
}

fn edge_receipt(
    rows: &[BulkEdgeRow],
    operation_uuid: OperationId,
    generation_uuid: Uuid,
) -> Result<RecordBatch, crate::GfError> {
    receipt(
        rows.iter().map(|row| {
            (
                row.row_ordinal,
                "edge",
                row.edge_uuid,
                None,
                Some(row.rel_type.as_str()),
                Some(row.source_uuid),
                Some(row.target_uuid),
            )
        }),
        rows.len(),
        operation_uuid,
        generation_uuid,
    )
}

type ReceiptRow<'a> = (
    u64,
    &'static str,
    Uuid,
    Option<&'a str>,
    Option<&'a str>,
    Option<Uuid>,
    Option<Uuid>,
);

pub(super) fn receipt<'a>(
    rows: impl Iterator<Item = ReceiptRow<'a>>,
    len: usize,
    operation_uuid: OperationId,
    generation_uuid: Uuid,
) -> Result<RecordBatch, crate::GfError> {
    if len == 0 {
        return Ok(RecordBatch::new_empty(bulk_receipt_schema()));
    }
    let rows = rows.collect::<Vec<_>>();
    let row_ordinals = UInt64Array::from_iter_values(rows.iter().map(|row| row.0));
    let entity_kinds = StringArray::from_iter_values(rows.iter().map(|row| row.1));
    let entity_uuids = uuid_array(rows.iter().map(|row| Some(row.2)))?;
    let labels = rows.iter().map(|row| row.3).collect::<StringArray>();
    let rel_types = rows.iter().map(|row| row.4).collect::<StringArray>();
    let source_uuids = uuid_array(rows.iter().map(|row| row.5))?;
    let target_uuids = uuid_array(rows.iter().map(|row| row.6))?;
    let operation_uuids = uuid_array((0..len).map(|_| Some(operation_uuid.0)))?;
    let generation_uuids = uuid_array((0..len).map(|_| Some(generation_uuid)))?;
    RecordBatch::try_new(
        bulk_receipt_schema(),
        vec![
            Arc::new(row_ordinals),
            Arc::new(entity_kinds),
            Arc::new(entity_uuids),
            Arc::new(labels),
            Arc::new(rel_types),
            Arc::new(source_uuids),
            Arc::new(target_uuids),
            Arc::new(operation_uuids),
            Arc::new(generation_uuids),
        ],
    )
    .map_err(|error| crate::GfError::Execution(error.to_string()))
}

fn uuid_array(
    values: impl IntoIterator<Item = Option<Uuid>>,
) -> Result<FixedSizeBinaryArray, crate::GfError> {
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        values.into_iter().map(|value| value.map(Uuid::into_bytes)),
        16,
    )
    .map_err(|error| crate::GfError::Execution(error.to_string()))
}

/// Canonical receipt schema used by the later publication slices.
///
/// Receipts retain input order and identify the created object, its node label
/// or edge relation/endpoints, the idempotency operation, and the one project
/// generation that published the batch. Kind-inapplicable fields are null.
#[must_use]
pub fn bulk_receipt_schema() -> SchemaRef {
    let metadata = contract_metadata("receipt");
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("row_ordinal", DataType::UInt64, false),
            Field::new("entity_kind", DataType::Utf8, false),
            Field::new("entity_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, true),
            Field::new("rel_type", DataType::Utf8, true),
            Field::new("source_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("target_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("operation_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "publication_generation_uuid",
                DataType::FixedSizeBinary(16),
                false,
            ),
        ],
        metadata,
    ))
}

impl GraphForge {
    /// Validate and atomically publish a logical Arrow node batch.
    ///
    /// Exact retries return the original ordered receipt without publishing a
    /// second generation. Reusing an operation UUID with changed normalized
    /// input returns `GF_IDEMPOTENCY_CONFLICT`.
    #[allow(
        clippy::too_many_lines,
        reason = "keeps validation, graph/catalog staging, publication, and rollback in one auditable transaction boundary"
    )]
    pub fn publish_bulk_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<RecordBatch, BulkNodePublicationError> {
        let _visibility = self.graph_visibility.lock()?;
        let normalized = self.normalize_bulk_nodes(operation_uuid, batches, false)?;
        if normalized.rows.is_empty() {
            return Ok(node_receipt(&normalized.rows, operation_uuid, Uuid::nil())?);
        }
        let generation_uuid = bulk_node_generation_uuid(operation_uuid, &normalized.rows);
        let root = self.resolved_generation.container_root();
        if let Some(published) =
            graphforge_storage::published_project_transaction(root, operation_uuid.0)?
        {
            if published.generation_uuid != generation_uuid {
                return Err(crate::GfError::Project {
                    code: graphforge_core::ProjectErrorCode::TransactionConflict,
                    message: "bulk-node operation UUID was already used with different input"
                        .into(),
                }
                .into());
            }
            return Ok(node_receipt(
                &normalized.rows,
                operation_uuid,
                generation_uuid,
            )?);
        }

        let mut candidates = normalized.identities().collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.dedup();
        let mut index = open_membership_index(self, BulkInputKind::Node)?;
        let mut existing = indexed_existing(
            index.as_mut(),
            &candidates,
            graphforge_storage::UuidIndexKind::Node,
            BulkInputKind::Node,
        )?;
        existing.extend(indexed_existing(
            index.as_mut(),
            &candidates,
            graphforge_storage::UuidIndexKind::Edge,
            BulkInputKind::Node,
        )?);
        if normalized
            .rows
            .iter()
            .any(|row| existing.contains(&row.node_uuid))
        {
            return Err(row_error(
                BulkInputKind::Node,
                BulkValidationReason::IdentityConflict,
                normalized
                    .rows
                    .iter()
                    .find(|row| existing.contains(&row.node_uuid))
                    .unwrap()
                    .row_ordinal,
                "node_uuid",
                "duplicate or existing UUID",
            )
            .into());
        }

        let prior_generation = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut next_catalog = prior_catalog.clone();
        let now = (self.clock.lock().expect("clock lock poisoned"))()?;
        let mut writer =
            graphforge_storage::GraphWriter::open_at(&self.dir(), self.ontology_mode, now)?;
        for row in &normalized.rows {
            let type_id = match self
                .ontology
                .as_ref()
                .and_then(|ontology| ontology.entity_type_id(&row.label))
            {
                Some(id) => graphforge_value::EntityTypeId::ontology(id)
                    .map_err(|error| graphforge_core::GfError::Validation(error.to_string()))?,
                None => {
                    graphforge_value::EntityTypeId::runtime(next_catalog.intern_label(&row.label)?)
                }
            };
            writer.create_node(row.node_uuid, type_id)?;
            let properties = row
                .properties
                .iter()
                .map(|(name, value)| {
                    next_catalog.intern_property(name, Some(&row.label))?;
                    Ok((name.clone(), crate::construction::prop_literal(value)?))
                })
                .collect::<Result<HashMap<_, _>, crate::GfError>>()?;
            if !properties.is_empty() {
                writer.set_properties(&row.node_uuid, Some(&row.label), properties)?;
            }
        }
        let expected_parent = normalized.source_generation_uuid;
        let publication = (|| -> Result<(), crate::GfError> {
            writer.flush()?;
            if self.path.is_some() {
                crate::persist_runtime_catalog(&self.dir(), &next_catalog)?;
            }
            let receipt = graphforge_exec::MutationReceipt {
                effects: vec![graphforge_exec::MutationEffect {
                    kind: graphforge_exec::MutationKind::CreateNode,
                    inputs: Vec::new(),
                    outputs: normalized
                        .rows
                        .iter()
                        .map(|row| graphforge_exec::MutationSubject {
                            uuid: row.node_uuid.into_bytes(),
                            kind: graphforge_exec::MutationSubjectKind::Node,
                        })
                        .collect(),
                }],
            };
            self.publish_graph_mutation_with_generation(
                &receipt,
                operation_uuid.0,
                generation_uuid,
                expected_parent,
                now,
            )
        })();
        if let Err(error) = publication {
            let still_prior = *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned")
                == expected_parent;
            if still_prior {
                crate::rematerialize_graph_workspace(&prior_generation, &self.dir())?;
            } else {
                *self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned") = next_catalog;
                self.adjacency_provider_for_session().invalidate();
            }
            return Err(error.into());
        }
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = next_catalog;
        self.adjacency_provider_for_session().invalidate();
        Ok(node_receipt(
            &normalized.rows,
            operation_uuid,
            generation_uuid,
        )?)
    }

    /// Validate and atomically publish a logical Arrow edge batch.
    ///
    /// Exact retries return the original ordered receipt without publishing a
    /// second generation. Reusing an operation UUID with changed normalized
    /// input returns `GF_IDEMPOTENCY_CONFLICT`.
    #[allow(
        clippy::too_many_lines,
        reason = "keeps validation, graph/catalog staging, publication, and rollback in one auditable transaction boundary"
    )]
    pub fn publish_bulk_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<RecordBatch, BulkEdgePublicationError> {
        let _visibility = self.graph_visibility.lock()?;
        let empty_nodes = ValidatedBulkNodes {
            rows: Vec::new(),
            operation_uuid,
            source_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
        };
        let normalized =
            self.normalize_bulk_edges(operation_uuid, batches, &empty_nodes, false, None)?;
        if normalized.rows.is_empty() {
            return Ok(edge_receipt(&normalized.rows, operation_uuid, Uuid::nil())?);
        }
        let generation_uuid = bulk_edge_generation_uuid(operation_uuid, &normalized.rows);
        let root = self.resolved_generation.container_root();
        if let Some(published) =
            graphforge_storage::published_project_transaction(root, operation_uuid.0)?
        {
            if published.generation_uuid != generation_uuid {
                return Err(crate::GfError::Project {
                    code: graphforge_core::ProjectErrorCode::TransactionConflict,
                    message: "bulk-edge operation UUID was already used with different input"
                        .into(),
                }
                .into());
            }
            return Ok(edge_receipt(
                &normalized.rows,
                operation_uuid,
                generation_uuid,
            )?);
        }

        let mut candidates = normalized
            .rows
            .iter()
            .map(|row| row.edge_uuid)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.dedup();
        let mut index = open_membership_index(self, BulkInputKind::Edge)?;
        let mut existing = indexed_existing(
            index.as_mut(),
            &candidates,
            graphforge_storage::UuidIndexKind::Edge,
            BulkInputKind::Edge,
        )?;
        existing.extend(indexed_existing(
            index.as_mut(),
            &candidates,
            graphforge_storage::UuidIndexKind::Node,
            BulkInputKind::Edge,
        )?);
        if let Some(row) = normalized
            .rows
            .iter()
            .find(|row| existing.contains(&row.edge_uuid))
        {
            return Err(row_error(
                BulkInputKind::Edge,
                BulkValidationReason::IdentityConflict,
                row.row_ordinal,
                "edge_uuid",
                "duplicate or existing UUID",
            )
            .into());
        }

        let prior_generation = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut next_catalog = prior_catalog.clone();
        let now = (self.clock.lock().expect("clock lock poisoned"))()?;
        let mut writer =
            graphforge_storage::GraphWriter::open_at(&self.dir(), self.ontology_mode, now)?;
        let endpoints = normalized
            .rows
            .iter()
            .flat_map(|row| [row.source_uuid, row.target_uuid])
            .collect::<BTreeSet<_>>();
        register_existing_endpoints(&mut writer, &self.dir(), &endpoints)?;
        for row in &normalized.rows {
            next_catalog.intern_relation_type(&row.rel_type)?;
            writer.create_edge(
                row.edge_uuid,
                &row.rel_type,
                &row.source_uuid,
                &row.target_uuid,
            )?;
            let properties = row
                .properties
                .iter()
                .map(|(name, value)| {
                    next_catalog.intern_property(name, Some(&row.rel_type))?;
                    Ok((name.clone(), crate::construction::prop_literal(value)?))
                })
                .collect::<Result<HashMap<_, _>, crate::GfError>>()?;
            if !properties.is_empty() {
                writer.set_edge_properties(&row.edge_uuid, Some(&row.rel_type), properties)?;
            }
        }
        let expected_parent = normalized.source_generation_uuid;
        let publication = (|| -> Result<(), crate::GfError> {
            writer.flush()?;
            if self.path.is_some() {
                crate::persist_runtime_catalog(&self.dir(), &next_catalog)?;
            }
            let receipt = graphforge_exec::MutationReceipt {
                effects: vec![graphforge_exec::MutationEffect {
                    kind: graphforge_exec::MutationKind::CreateEdge,
                    inputs: normalized
                        .rows
                        .iter()
                        .flat_map(|row| [row.source_uuid, row.target_uuid])
                        .map(|uuid| graphforge_exec::MutationSubject {
                            uuid: uuid.into_bytes(),
                            kind: graphforge_exec::MutationSubjectKind::Node,
                        })
                        .collect(),
                    outputs: normalized
                        .rows
                        .iter()
                        .map(|row| graphforge_exec::MutationSubject {
                            uuid: row.edge_uuid.into_bytes(),
                            kind: graphforge_exec::MutationSubjectKind::Edge,
                        })
                        .collect(),
                }],
            };
            self.publish_graph_mutation_with_generation(
                &receipt,
                operation_uuid.0,
                generation_uuid,
                expected_parent,
                now,
            )
        })();
        if let Err(error) = publication {
            let still_prior = *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned")
                == expected_parent;
            if still_prior {
                crate::rematerialize_graph_workspace(&prior_generation, &self.dir())?;
            } else {
                *self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned") = next_catalog;
                self.adjacency_provider_for_session().invalidate();
            }
            return Err(error.into());
        }
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = next_catalog;
        self.adjacency_provider_for_session().invalidate();
        Ok(edge_receipt(
            &normalized.rows,
            operation_uuid,
            generation_uuid,
        )?)
    }
}

#[cfg(test)]
mod tests;
