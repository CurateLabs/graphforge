//! Rust-owned Arrow contracts and zero-write validation for bulk construction.
//!
//! The required topology columns precede property columns, which are sorted by
//! name. Identity columns are nullable `FixedSizeBinary(16)`: a UUIDv7 is
//! explicit, while null deterministically derives a UUIDv7 from operation
//! identity, entity kind, and logical row ordinal. Edge endpoints are always
//! explicit non-null UUIDv7 values. A logical request is the concatenation
//! of its record batches; [`BulkNodeRow::row_ordinal`] and
//! [`BulkEdgeRow::row_ordinal`] refer to that order regardless of partitioning.
//!
//! Validation is deliberately publication-free. It checks schemas, existing
//! and request-local identities, endpoints, identifiers, and property columns
//! in deterministic row/field order. Failures expose a stable reason plus
//! optional batch, logical-row, and field coordinates. The
//! publication methods that consume these validated values are separate APIs.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeListArray, LargeStringArray, ListArray, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use graphforge_core::uuid::Uuid;
use graphforge_ontology::PropertyValueType;
use sha2::{Digest, Sha256};

use super::{GraphForge, OntologyMode, OperationId, PropValue};

/// Failure from canonical bulk-node publication.
#[derive(Debug, thiserror::Error)]
pub enum BulkNodePublicationError {
    /// Complete input validation failed before any write.
    #[error(transparent)]
    Validation(#[from] BulkValidationError),
    /// Storage or project publication failed; the prior generation remains visible.
    #[error(transparent)]
    Publication(#[from] super::GfError),
}

/// Failure from canonical bulk-edge publication.
#[derive(Debug, thiserror::Error)]
pub enum BulkEdgePublicationError {
    /// Complete input validation failed before any write.
    #[error(transparent)]
    Validation(#[from] BulkValidationError),
    /// Storage or project publication failed; the prior generation remains visible.
    #[error(transparent)]
    Publication(#[from] super::GfError),
}

/// Version of the Arrow input, validation, and receipt contract.
pub const BULK_CONSTRUCTION_CONTRACT_VERSION: u32 = 1;

/// Bulk input family associated with a validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BulkInputKind {
    /// Node input contract.
    Node,
    /// Edge input contract.
    Edge,
}

impl BulkInputKind {
    /// Stable external spelling used by thin bindings.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }
}

/// Stable machine-readable reason for a bulk validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BulkValidationReason {
    /// Arrow fields or contract metadata do not match the canonical schema.
    SchemaMismatch,
    /// A property attempts to use a reserved topology field.
    ReservedField,
    /// A property field occurs more than once.
    DuplicateField,
    /// The Arrow property type cannot project to the public value model.
    UnsupportedPropertyType,
    /// A label, relation, or property identifier is malformed.
    InvalidIdentifier,
    /// An explicit identity is not a UUIDv7.
    InvalidUuid,
    /// An identity collides with existing or request-local content.
    IdentityConflict,
    /// An edge endpoint is absent from the pinned graph and request nodes.
    MissingEndpoint,
    /// A strict ontology does not declare the requested owner type.
    UnknownOntologyType,
    /// A strict ontology does not declare the property for its owner.
    UnknownOntologyProperty,
    /// A property Arrow type does not normalize to the ontology type.
    PropertyTypeMismatch,
    /// Arrow nullability violates the strict ontology declaration.
    NullabilityMismatch,
    /// A dependent request was validated against another generation.
    GenerationMismatch,
    /// Existing project state could not be read or decoded.
    ProjectState,
    /// The logical input row ordinal exceeded the public `u64` contract.
    OrdinalOverflow,
}

impl BulkValidationReason {
    /// Stable external spelling used by thin bindings.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchemaMismatch => "schema_mismatch",
            Self::ReservedField => "reserved_field",
            Self::DuplicateField => "duplicate_field",
            Self::UnsupportedPropertyType => "unsupported_property_type",
            Self::InvalidIdentifier => "invalid_identifier",
            Self::InvalidUuid => "invalid_uuid",
            Self::IdentityConflict => "identity_conflict",
            Self::MissingEndpoint => "missing_endpoint",
            Self::UnknownOntologyType => "unknown_ontology_type",
            Self::UnknownOntologyProperty => "unknown_ontology_property",
            Self::PropertyTypeMismatch => "property_type_mismatch",
            Self::NullabilityMismatch => "nullability_mismatch",
            Self::GenerationMismatch => "generation_mismatch",
            Self::ProjectState => "project_state",
            Self::OrdinalOverflow => "ordinal_overflow",
        }
    }
}

/// Machine-readable bulk validation error with deterministic input context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkValidationError {
    /// Input contract being validated.
    pub kind: BulkInputKind,
    /// Stable closed reason code.
    pub reason: BulkValidationReason,
    /// Zero-based record-batch index for schema-scoped failures.
    pub batch_index: Option<u64>,
    /// Zero-based logical row across all input record batches.
    pub row_ordinal: Option<u64>,
    /// Canonical field name, when one field owns the failure.
    pub field: Option<String>,
    /// Stable safe diagnostic without row values.
    pub message: String,
}

impl BulkValidationError {
    /// Stable public error class shared by every bulk validation failure.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        "GF_BULK_VALIDATION"
    }
}

impl std::fmt::Display for BulkValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "GF_BULK_VALIDATION({}): bulk {}",
            self.reason.as_str(),
            self.kind.as_str()
        )?;
        if let Some(batch) = self.batch_index {
            write!(formatter, " batch {batch}")?;
        }
        if let Some(row) = self.row_ordinal {
            write!(formatter, " row {row}")?;
        }
        if let Some(field) = &self.field {
            write!(formatter, " field {field:?}")?;
        }
        write!(formatter, ": {}", self.message)
    }
}

impl std::error::Error for BulkValidationError {}

const NODE_REQUIRED: [(&str, DataType, bool); 2] = [
    ("node_uuid", DataType::FixedSizeBinary(16), true),
    ("label", DataType::Utf8, false),
];
const EDGE_REQUIRED: [(&str, DataType, bool); 4] = [
    ("edge_uuid", DataType::FixedSizeBinary(16), true),
    ("rel_type", DataType::Utf8, false),
    ("source_uuid", DataType::FixedSizeBinary(16), false),
    ("target_uuid", DataType::FixedSizeBinary(16), false),
];

/// One fully normalized node row ready for a later atomic publication slice.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkNodeRow {
    /// Zero-based ordinal across the logical concatenation of all input batches.
    pub row_ordinal: u64,
    /// Stable caller-supplied UUIDv7.
    pub node_uuid: Uuid,
    /// Primary node label.
    pub label: String,
    /// Lexicographically ordered dynamic property columns.
    pub properties: BTreeMap<String, PropValue>,
}

/// One fully normalized edge row ready for a later atomic publication slice.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkEdgeRow {
    /// Zero-based ordinal across the logical concatenation of all input batches.
    pub row_ordinal: u64,
    /// Stable caller-supplied UUIDv7.
    pub edge_uuid: Uuid,
    /// Relationship type.
    pub rel_type: String,
    /// Existing or same-request source node UUID.
    pub source_uuid: Uuid,
    /// Existing or same-request target node UUID.
    pub target_uuid: Uuid,
    /// Lexicographically ordered dynamic property columns.
    pub properties: BTreeMap<String, PropValue>,
}

/// A complete validated node request. Constructed only after every row passes.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBulkNodes {
    rows: Vec<BulkNodeRow>,
    operation_uuid: OperationId,
    source_generation_uuid: Uuid,
}

impl ValidatedBulkNodes {
    /// Validated rows in deterministic input order.
    #[must_use]
    pub fn rows(&self) -> &[BulkNodeRow] {
        &self.rows
    }

    /// Committed project generation against which identities were validated.
    #[must_use]
    pub fn source_generation_uuid(&self) -> Uuid {
        self.source_generation_uuid
    }

    /// Exact idempotency identity used for deterministic generated UUIDs.
    #[must_use]
    pub fn operation_uuid(&self) -> OperationId {
        self.operation_uuid
    }

    fn identities(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.rows.iter().map(|row| row.node_uuid)
    }
}

/// A complete validated edge request. Constructed only after every row passes.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBulkEdges {
    rows: Vec<BulkEdgeRow>,
    operation_uuid: OperationId,
    source_generation_uuid: Uuid,
}

impl ValidatedBulkEdges {
    /// Validated rows in deterministic input order.
    #[must_use]
    pub fn rows(&self) -> &[BulkEdgeRow] {
        &self.rows
    }

    /// Committed project generation against which identities were validated.
    #[must_use]
    pub fn source_generation_uuid(&self) -> Uuid {
        self.source_generation_uuid
    }

    /// Exact idempotency identity used for deterministic generated UUIDs.
    #[must_use]
    pub fn operation_uuid(&self) -> OperationId {
        self.operation_uuid
    }
}

/// Build the canonical node input schema from caller property columns.
///
/// Required fields are nullable `node_uuid: FixedSizeBinary(16)` (null means
/// deterministic generation) and non-null `label: Utf8`. Property columns are
/// sorted by name so schema identity is independent of map iteration.
pub fn bulk_node_input_schema(properties: Vec<Field>) -> Result<SchemaRef, BulkValidationError> {
    input_schema(BulkInputKind::Node, &NODE_REQUIRED, properties)
}

/// Build the canonical edge input schema from caller property columns.
///
/// Required fields are nullable `edge_uuid` (null means deterministic
/// generation), explicit non-null `source_uuid` and `target_uuid`, plus
/// non-null `rel_type: Utf8`.
pub fn bulk_edge_input_schema(properties: Vec<Field>) -> Result<SchemaRef, BulkValidationError> {
    input_schema(BulkInputKind::Edge, &EDGE_REQUIRED, properties)
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
    pub(crate) fn import_base_membership(
        &self,
        candidates: &[Uuid],
        kind: graphforge_storage::UuidIndexKind,
        input_kind: BulkInputKind,
    ) -> Result<Vec<bool>, BulkValidationError> {
        let mut index = open_membership_index(self, input_kind)?;
        let Some(index) = index.as_mut() else {
            return Ok(vec![false; candidates.len()]);
        };
        index
            .probe(kind, candidates)
            .map(|(found, _)| found)
            .map_err(|error| {
                contract_error(
                    input_kind,
                    BulkValidationReason::ProjectState,
                    &error.to_string(),
                )
            })
    }

    pub(crate) fn normalize_import_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        self.normalize_bulk_nodes(operation_uuid, batches, false)
    }

    pub(crate) fn normalize_import_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        imported_endpoints: &BTreeSet<Uuid>,
    ) -> Result<ValidatedBulkEdges, BulkValidationError> {
        let empty_nodes = ValidatedBulkNodes {
            rows: Vec::new(),
            operation_uuid,
            source_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
        };
        self.normalize_bulk_edges(
            operation_uuid,
            batches,
            &empty_nodes,
            false,
            Some(imported_endpoints),
        )
    }

    /// Normalize and validate every Arrow node row without writing storage,
    /// the runtime catalog, ontology state, or project generations.
    pub fn validate_bulk_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        let _visibility = self.graph_visibility.read().map_err(|error| {
            contract_error(
                BulkInputKind::Node,
                BulkValidationReason::ProjectState,
                &error.to_string(),
            )
        })?;
        self.normalize_bulk_nodes(operation_uuid, batches, true)
    }

    fn normalize_bulk_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        reject_existing: bool,
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        let source_generation_uuid = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        validate_operation_uuid(BulkInputKind::Node, operation_uuid)?;
        validate_partition_schemas(BulkInputKind::Node, &NODE_REQUIRED, batches)?;
        let mut existing = BTreeSet::new();
        if reject_existing {
            let candidates = candidate_uuids(batches, BulkInputKind::Node, "node_uuid")?;
            let mut index = open_membership_index(self, BulkInputKind::Node)?;
            existing = indexed_existing(
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
        }
        let mut observed = BTreeSet::new();
        let mut rows = Vec::new();
        let mut ordinal = 0_u64;

        for batch in batches {
            let uuids = uuid_column(batch, BulkInputKind::Node, "node_uuid")?;
            let labels = string_column(batch, BulkInputKind::Node, "label")?;
            let properties = property_columns(batch, &NODE_REQUIRED);
            preflight_spatial_columns(BulkInputKind::Node, ordinal, &properties)?;
            for row in 0..batch.num_rows() {
                let node_uuid = uuid_at(
                    uuids,
                    row,
                    BulkInputKind::Node,
                    ordinal,
                    "node_uuid",
                    operation_uuid,
                )?;
                if existing.contains(&node_uuid) || !observed.insert(node_uuid) {
                    return Err(row_error(
                        BulkInputKind::Node,
                        BulkValidationReason::IdentityConflict,
                        ordinal,
                        "node_uuid",
                        "duplicate or existing UUID",
                    ));
                }
                if labels.is_null(row) {
                    return Err(row_error(
                        BulkInputKind::Node,
                        BulkValidationReason::SchemaMismatch,
                        ordinal,
                        "label",
                        "value is null",
                    ));
                }
                let label = labels.value(row);
                validate_identifier(BulkInputKind::Node, ordinal, "label", label)?;
                validate_node_owner(self, ordinal, label)?;
                let values = normalize_properties(
                    BulkInputKind::Node,
                    ordinal,
                    row,
                    &properties,
                    |name, field| validate_node_property(self, ordinal, label, name, field),
                )?;
                rows.push(BulkNodeRow {
                    row_ordinal: ordinal,
                    node_uuid,
                    label: label.to_owned(),
                    properties: values,
                });
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    contract_error(
                        BulkInputKind::Node,
                        BulkValidationReason::OrdinalOverflow,
                        "logical row ordinal overflow",
                    )
                })?;
            }
        }
        Ok(ValidatedBulkNodes {
            rows,
            operation_uuid,
            source_generation_uuid,
        })
    }

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
                return Err(super::GfError::Project {
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

        let candidates = normalized.identities().collect::<BTreeSet<_>>();
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
            graphforge_storage::GraphWriter::open_at(&self.dir, self.ontology_mode, now)?;
        for row in &normalized.rows {
            let type_id = self
                .ontology
                .as_ref()
                .and_then(|ontology| ontology.entity_type_id(&row.label))
                .unwrap_or_else(|| {
                    graphforge_ir::runtime_entity_type_id(next_catalog.intern_label(&row.label))
                });
            writer.create_node(row.node_uuid, type_id)?;
            let properties = row
                .properties
                .iter()
                .map(|(name, value)| {
                    next_catalog.intern_property(name, Some(&row.label));
                    Ok((name.clone(), crate::construction::prop_literal(value)?))
                })
                .collect::<Result<HashMap<_, _>, super::GfError>>()?;
            if !properties.is_empty() {
                writer.set_properties(&row.node_uuid, Some(&row.label), properties)?;
            }
        }
        let expected_parent = normalized.source_generation_uuid;
        let publication = (|| -> Result<(), super::GfError> {
            writer.flush()?;
            if self.path.is_some() {
                super::persist_runtime_catalog(&self.dir, &next_catalog)?;
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
                crate::rematerialize_graph_workspace(&prior_generation, &self.dir)?;
            } else {
                *self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned") = next_catalog;
                self.adjacency_provider.invalidate();
            }
            return Err(error.into());
        }
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = next_catalog;
        self.adjacency_provider.invalidate();
        Ok(node_receipt(
            &normalized.rows,
            operation_uuid,
            generation_uuid,
        )?)
    }

    /// Normalize and validate every Arrow edge row without writing storage.
    /// Endpoints may reference existing nodes or nodes in `same_request_nodes`.
    pub fn validate_bulk_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        same_request_nodes: &ValidatedBulkNodes,
    ) -> Result<ValidatedBulkEdges, BulkValidationError> {
        let _visibility = self.graph_visibility.read().map_err(|error| {
            contract_error(
                BulkInputKind::Edge,
                BulkValidationReason::ProjectState,
                &error.to_string(),
            )
        })?;
        self.normalize_bulk_edges(operation_uuid, batches, same_request_nodes, true, None)
    }

    fn normalize_bulk_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        same_request_nodes: &ValidatedBulkNodes,
        reject_existing: bool,
        additional_known_nodes: Option<&BTreeSet<Uuid>>,
    ) -> Result<ValidatedBulkEdges, BulkValidationError> {
        let source_generation_uuid = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        validate_operation_uuid(BulkInputKind::Edge, operation_uuid)?;
        if same_request_nodes.source_generation_uuid != source_generation_uuid {
            return Err(contract_error(
                BulkInputKind::Edge,
                BulkValidationReason::GenerationMismatch,
                "same-request nodes were validated against a different graph generation",
            ));
        }
        validate_partition_schemas(BulkInputKind::Edge, &EDGE_REQUIRED, batches)?;
        let endpoint_candidates = candidate_endpoint_uuids(batches)?;
        let edge_candidates = reject_existing
            .then(|| candidate_uuids(batches, BulkInputKind::Edge, "edge_uuid"))
            .transpose()?;
        let (mut known_nodes, existing_edges) =
            existing_edge_context(self, &endpoint_candidates, edge_candidates.as_ref())?;
        if let Some(additional) = additional_known_nodes {
            known_nodes.extend(additional.iter().copied());
        }
        known_nodes.extend(same_request_nodes.identities());
        let mut observed = BTreeSet::new();
        let mut rows = Vec::new();
        let mut ordinal = 0_u64;

        for batch in batches {
            let uuids = uuid_column(batch, BulkInputKind::Edge, "edge_uuid")?;
            let rel_types = string_column(batch, BulkInputKind::Edge, "rel_type")?;
            let sources = uuid_column(batch, BulkInputKind::Edge, "source_uuid")?;
            let targets = uuid_column(batch, BulkInputKind::Edge, "target_uuid")?;
            let properties = property_columns(batch, &EDGE_REQUIRED);
            preflight_spatial_columns(BulkInputKind::Edge, ordinal, &properties)?;
            for row in 0..batch.num_rows() {
                let edge_uuid = uuid_at(
                    uuids,
                    row,
                    BulkInputKind::Edge,
                    ordinal,
                    "edge_uuid",
                    operation_uuid,
                )?;
                validate_edge_identity(
                    edge_uuid,
                    &known_nodes,
                    &existing_edges,
                    &mut observed,
                    ordinal,
                )?;
                let source_uuid =
                    explicit_uuid_at(sources, row, BulkInputKind::Edge, ordinal, "source_uuid")?;
                validate_edge_endpoint(source_uuid, &known_nodes, ordinal, "source_uuid")?;
                let target_uuid =
                    explicit_uuid_at(targets, row, BulkInputKind::Edge, ordinal, "target_uuid")?;
                validate_edge_endpoint(target_uuid, &known_nodes, ordinal, "target_uuid")?;
                if rel_types.is_null(row) {
                    return Err(row_error(
                        BulkInputKind::Edge,
                        BulkValidationReason::SchemaMismatch,
                        ordinal,
                        "rel_type",
                        "value is null",
                    ));
                }
                let rel_type = rel_types.value(row);
                validate_identifier(BulkInputKind::Edge, ordinal, "rel_type", rel_type)?;
                validate_edge_owner(self, ordinal, rel_type)?;
                let values = normalize_properties(
                    BulkInputKind::Edge,
                    ordinal,
                    row,
                    &properties,
                    |name, field| validate_edge_property(self, ordinal, rel_type, name, field),
                )?;
                rows.push(BulkEdgeRow {
                    row_ordinal: ordinal,
                    edge_uuid,
                    rel_type: rel_type.to_owned(),
                    source_uuid,
                    target_uuid,
                    properties: values,
                });
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    contract_error(
                        BulkInputKind::Edge,
                        BulkValidationReason::OrdinalOverflow,
                        "logical row ordinal overflow",
                    )
                })?;
            }
        }
        Ok(ValidatedBulkEdges {
            rows,
            operation_uuid,
            source_generation_uuid,
        })
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
                return Err(super::GfError::Project {
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

        let candidates = normalized
            .rows
            .iter()
            .map(|row| row.edge_uuid)
            .collect::<BTreeSet<_>>();
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
            graphforge_storage::GraphWriter::open_at(&self.dir, self.ontology_mode, now)?;
        let endpoints = normalized
            .rows
            .iter()
            .flat_map(|row| [row.source_uuid, row.target_uuid])
            .collect::<BTreeSet<_>>();
        register_existing_endpoints(&mut writer, &self.dir, &endpoints)?;
        for row in &normalized.rows {
            next_catalog.intern_relation_type(&row.rel_type);
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
                    next_catalog.intern_property(name, Some(&row.rel_type));
                    Ok((name.clone(), crate::construction::prop_literal(value)?))
                })
                .collect::<Result<HashMap<_, _>, super::GfError>>()?;
            if !properties.is_empty() {
                writer.set_edge_properties(&row.edge_uuid, Some(&row.rel_type), properties)?;
            }
        }
        let expected_parent = normalized.source_generation_uuid;
        let publication = (|| -> Result<(), super::GfError> {
            writer.flush()?;
            if self.path.is_some() {
                super::persist_runtime_catalog(&self.dir, &next_catalog)?;
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
                crate::rematerialize_graph_workspace(&prior_generation, &self.dir)?;
            } else {
                *self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned") = next_catalog;
                self.adjacency_provider.invalidate();
            }
            return Err(error.into());
        }
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = next_catalog;
        self.adjacency_provider.invalidate();
        Ok(edge_receipt(
            &normalized.rows,
            operation_uuid,
            generation_uuid,
        )?)
    }
}

fn input_schema(
    kind: BulkInputKind,
    required: &[(&str, DataType, bool)],
    mut properties: Vec<Field>,
) -> Result<SchemaRef, BulkValidationError> {
    properties.sort_unstable_by(|left, right| left.name().cmp(right.name()));
    let mut prior = None;
    for field in &properties {
        if required.iter().any(|(name, _, _)| *name == field.name()) {
            return Err(field_error(
                kind,
                BulkValidationReason::ReservedField,
                field.name(),
                "property name is reserved by the topology contract",
            ));
        }
        validate_property_name(kind, field.name())?;
        if field.metadata().contains_key("ARROW:extension:name") {
            spatial_type_from_field(field).ok_or_else(|| {
                field_error(
                    kind,
                    BulkValidationReason::UnsupportedPropertyType,
                    field.name(),
                    "unsupported or non-canonical Arrow extension property",
                )
            })?;
        } else {
            validate_property_type(kind, field.name(), field.data_type())?;
        }
        if prior == Some(field.name()) {
            return Err(field_error(
                kind,
                BulkValidationReason::DuplicateField,
                field.name(),
                "property field is duplicated",
            ));
        }
        prior = Some(field.name());
    }
    let fields = required
        .iter()
        .map(|(name, data_type, nullable)| Field::new(*name, data_type.clone(), *nullable))
        .chain(properties)
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        contract_metadata(kind.as_str()),
    )))
}

fn contract_metadata(kind: &str) -> HashMap<String, String> {
    HashMap::from([
        (
            "graphforge.bulk_contract_version".to_owned(),
            BULK_CONSTRUCTION_CONTRACT_VERSION.to_string(),
        ),
        ("graphforge.bulk_kind".to_owned(), kind.to_owned()),
        (
            "graphforge.row_order".to_owned(),
            "logical_input_order".to_owned(),
        ),
    ])
}

fn validate_partition_schemas(
    kind: BulkInputKind,
    required: &[(&str, DataType, bool)],
    batches: &[RecordBatch],
) -> Result<(), BulkValidationError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    validate_batch_schema(kind, 0, required, first.schema().as_ref())?;
    for (index, batch) in batches.iter().enumerate().skip(1) {
        validate_batch_schema(kind, index, required, batch.schema().as_ref())?;
        if batch.schema().as_ref() != first.schema().as_ref() {
            return Err(batch_error(
                kind,
                index,
                BulkValidationReason::SchemaMismatch,
                None,
                "schema differs from batch 0",
            ));
        }
    }
    Ok(())
}

fn validate_batch_schema(
    kind: BulkInputKind,
    batch_index: usize,
    required: &[(&str, DataType, bool)],
    schema: &Schema,
) -> Result<(), BulkValidationError> {
    if schema.metadata() != &contract_metadata(kind.as_str()) {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "contract version/kind/order metadata is not canonical",
        ));
    }
    if schema.fields().len() < required.len() {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "required topology fields are missing",
        ));
    }
    for (index, (name, expected, nullable)) in required.iter().enumerate() {
        let field = &schema.fields()[index];
        if field.name() != name || field.data_type() != expected || field.is_nullable() != *nullable
        {
            return Err(batch_error(
                kind,
                batch_index,
                BulkValidationReason::SchemaMismatch,
                Some(name),
                &format!("field {index} must be {name:?}: {expected} nullable={nullable}"),
            ));
        }
    }
    let properties = schema
        .fields()
        .iter()
        .skip(required.len())
        .collect::<Vec<_>>();
    if !properties
        .windows(2)
        .all(|pair| pair[0].name() < pair[1].name())
    {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "property fields must be unique and lexicographically ordered",
        ));
    }
    for field in properties {
        validate_property_name(kind, field.name())?;
        if field.metadata().contains_key("ARROW:extension:name") {
            spatial_type_from_field(field).ok_or_else(|| {
                field_error(
                    kind,
                    BulkValidationReason::UnsupportedPropertyType,
                    field.name(),
                    "unsupported or non-canonical Arrow extension property",
                )
            })?;
        } else {
            validate_property_type(kind, field.name(), field.data_type())?;
        }
    }
    Ok(())
}

fn property_columns<'a>(
    batch: &'a RecordBatch,
    required: &[(&str, DataType, bool)],
) -> Vec<(&'a Field, &'a ArrayRef)> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .enumerate()
        .skip(required.len())
        .map(|(index, field)| (field.as_ref(), batch.column(index)))
        .collect()
}

fn validate_edge_identity(
    edge_uuid: Uuid,
    known_nodes: &BTreeSet<Uuid>,
    existing_edges: &BTreeSet<Uuid>,
    observed: &mut BTreeSet<Uuid>,
    ordinal: u64,
) -> Result<(), BulkValidationError> {
    if known_nodes.contains(&edge_uuid)
        || existing_edges.contains(&edge_uuid)
        || !observed.insert(edge_uuid)
    {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::IdentityConflict,
            ordinal,
            "edge_uuid",
            "duplicate or existing UUID",
        ));
    }
    Ok(())
}

fn validate_edge_endpoint(
    endpoint_uuid: Uuid,
    known_nodes: &BTreeSet<Uuid>,
    ordinal: u64,
    field: &str,
) -> Result<(), BulkValidationError> {
    if !known_nodes.contains(&endpoint_uuid) {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::MissingEndpoint,
            ordinal,
            field,
            "endpoint does not exist",
        ));
    }
    Ok(())
}

fn normalize_properties<F>(
    kind: BulkInputKind,
    ordinal: u64,
    row: usize,
    columns: &[(&Field, &ArrayRef)],
    mut owner_validation: F,
) -> Result<BTreeMap<String, PropValue>, BulkValidationError>
where
    F: FnMut(&str, &Field) -> Result<(), BulkValidationError>,
{
    let mut values = BTreeMap::new();
    for (field, array) in columns {
        owner_validation(field.name(), field)?;
        let value = if field.metadata().contains_key("ARROW:extension:name") {
            spatial_type_from_field(field).ok_or_else(|| {
                row_error(
                    kind,
                    BulkValidationReason::PropertyTypeMismatch,
                    ordinal,
                    field.name(),
                    "spatial field metadata is not canonical",
                )
            })?;
            if array.is_null(row) {
                PropValue::Null
            } else {
                PropValue::Spatial(
                    graphforge_storage::decode_spatial_property_value(array.as_ref(), field, row)
                        .map_err(|error| {
                        row_error(
                            kind,
                            BulkValidationReason::UnsupportedPropertyType,
                            ordinal,
                            field.name(),
                            &error.to_string(),
                        )
                    })?,
                )
            }
        } else {
            property_value_at(array.as_ref(), row).map_err(|message| {
                row_error(
                    kind,
                    BulkValidationReason::UnsupportedPropertyType,
                    ordinal,
                    field.name(),
                    &message,
                )
            })?
        };
        values.insert(field.name().to_owned(), value);
    }
    Ok(values)
}

fn preflight_spatial_columns(
    kind: BulkInputKind,
    first_ordinal: u64,
    columns: &[(&Field, &ArrayRef)],
) -> Result<(), BulkValidationError> {
    for (field, array) in columns {
        let Some(spatial) = spatial_type_from_field(field) else {
            continue;
        };
        spatial
            .validate_array(
                field,
                array.as_ref(),
                graphforge_ontology::SpatialValidationLimits::default(),
            )
            .map_err(|error| {
                row_error(
                    kind,
                    BulkValidationReason::PropertyTypeMismatch,
                    first_ordinal,
                    field.name(),
                    error.code(),
                )
            })?;
    }
    Ok(())
}

fn spatial_type_from_field(field: &Field) -> Option<graphforge_ontology::SpatialType> {
    use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};
    let geometry = match field.metadata().get("ARROW:extension:name")?.as_str() {
        "geoarrow.point" => SpatialGeometryType::Point,
        "geoarrow.linestring" => SpatialGeometryType::LineString,
        "geoarrow.polygon" => SpatialGeometryType::Polygon,
        "geoarrow.multipoint" => SpatialGeometryType::MultiPoint,
        "geoarrow.multilinestring" => SpatialGeometryType::MultiLineString,
        "geoarrow.multipolygon" => SpatialGeometryType::MultiPolygon,
        _ => return None,
    };
    let metadata = field.metadata().get("ARROW:extension:metadata")?;
    let crs = if metadata == &SpatialCrs::Epsg4326.extension_metadata() {
        SpatialCrs::Epsg4326
    } else if metadata == &SpatialCrs::Epsg3857.extension_metadata() {
        SpatialCrs::Epsg3857
    } else {
        return None;
    };
    Some(SpatialType { geometry, crs })
}

fn property_value_at(array: &dyn Array, row: usize) -> Result<PropValue, String> {
    if array.is_null(row) {
        return Ok(PropValue::Null);
    }
    macro_rules! scalar {
        ($ty:ty, $value:expr) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|values| $value(values.value(row)))
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())
        };
    }
    match array.data_type() {
        DataType::Boolean => scalar!(BooleanArray, PropValue::Bool),
        DataType::Int8 => scalar!(Int8Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int16 => scalar!(Int16Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int32 => scalar!(Int32Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int64 => scalar!(Int64Array, PropValue::Int),
        DataType::UInt8 => scalar!(UInt8Array, |value| PropValue::Int(i64::from(value))),
        DataType::UInt16 => scalar!(UInt16Array, |value| PropValue::Int(i64::from(value))),
        DataType::UInt32 => scalar!(UInt32Array, |value| PropValue::Int(i64::from(value))),
        DataType::Float32 => scalar!(Float32Array, |value| PropValue::Float(f64::from(value))),
        DataType::Float64 => scalar!(Float64Array, PropValue::Float),
        DataType::Utf8 => scalar!(StringArray, |value: &str| PropValue::Str(value.to_owned())),
        DataType::LargeUtf8 => {
            scalar!(LargeStringArray, |value: &str| PropValue::Str(
                value.to_owned()
            ))
        }
        DataType::List(_) => {
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())?
                .value(row);
            (0..values.len())
                .map(|index| property_value_at(values.as_ref(), index))
                .collect::<Result<Vec<_>, _>>()
                .map(PropValue::List)
        }
        DataType::LargeList(_) => {
            let values = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())?
                .value(row);
            (0..values.len())
                .map(|index| property_value_at(values.as_ref(), index))
                .collect::<Result<Vec<_>, _>>()
                .map(PropValue::List)
        }
        other => Err(format!("unsupported property type {other}")),
    }
}

fn validate_node_owner(
    graph: &GraphForge,
    ordinal: u64,
    label: &str,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode == OntologyMode::Strict
        && graph
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label))
            .is_none()
    {
        return Err(row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "label",
            "unknown strict ontology entity type",
        ));
    }
    Ok(())
}

fn validate_edge_owner(
    graph: &GraphForge,
    ordinal: u64,
    rel_type: &str,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode == OntologyMode::Strict
        && graph
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.relation_type_id(rel_type))
            .is_none()
    {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "rel_type",
            "unknown strict ontology relationship type",
        ));
    }
    Ok(())
}

fn validate_node_property(
    graph: &GraphForge,
    ordinal: u64,
    label: &str,
    name: &str,
    field: &Field,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode != OntologyMode::Strict {
        return Ok(());
    }
    let ontology = graph.ontology.as_ref().ok_or_else(|| {
        contract_error(
            BulkInputKind::Node,
            BulkValidationReason::ProjectState,
            "strict project has no ontology",
        )
    })?;
    let owner = ontology.entity_type_id(label).ok_or_else(|| {
        row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "label",
            "unknown strict ontology entity type",
        )
    })?;
    let definition = ontology.entity_property_def(owner, name).ok_or_else(|| {
        row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyProperty,
            ordinal,
            name,
            "property is not declared for strict entity type",
        )
    })?;
    validate_ontology_field(
        BulkInputKind::Node,
        ordinal,
        name,
        field,
        &definition.value_type,
        definition.nullable,
    )
}

fn validate_edge_property(
    graph: &GraphForge,
    ordinal: u64,
    rel_type: &str,
    name: &str,
    field: &Field,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode != OntologyMode::Strict {
        return Ok(());
    }
    let ontology = graph.ontology.as_ref().ok_or_else(|| {
        contract_error(
            BulkInputKind::Edge,
            BulkValidationReason::ProjectState,
            "strict project has no ontology",
        )
    })?;
    let owner = ontology.relation_type_id(rel_type).ok_or_else(|| {
        row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "rel_type",
            "unknown strict ontology relationship type",
        )
    })?;
    let definition = ontology.relation_property_def(owner, name).ok_or_else(|| {
        row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyProperty,
            ordinal,
            name,
            "property is not declared for strict relationship type",
        )
    })?;
    validate_ontology_field(
        BulkInputKind::Edge,
        ordinal,
        name,
        field,
        &definition.value_type,
        definition.nullable,
    )
}

fn validate_ontology_field(
    kind: BulkInputKind,
    ordinal: u64,
    name: &str,
    field: &Field,
    expected: &PropertyValueType,
    nullable: bool,
) -> Result<(), BulkValidationError> {
    let expected_arrow = graphforge_storage::property_type_to_arrow(expected);
    let compatible = match expected {
        PropertyValueType::Utf8 => {
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        }
        PropertyValueType::Int64 => matches!(
            field.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        ),
        PropertyValueType::Float64 => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        PropertyValueType::Bool => matches!(field.data_type(), DataType::Boolean),
        PropertyValueType::List => {
            matches!(
                field.data_type(),
                DataType::List(_) | DataType::LargeList(_)
            )
        }
        PropertyValueType::Spatial(spatial) => {
            field.data_type() == &spatial.data_type()
                && field.metadata() == &spatial.field_metadata()
        }
        PropertyValueType::Duration | PropertyValueType::DateTime | PropertyValueType::Map => false,
    };
    if !compatible {
        return Err(row_error(
            kind,
            BulkValidationReason::PropertyTypeMismatch,
            ordinal,
            name,
            &format!(
                "property type {} does not match strict ontology type {expected_arrow}",
                field.data_type()
            ),
        ));
    }
    if field.is_nullable() && !nullable {
        return Err(row_error(
            kind,
            BulkValidationReason::NullabilityMismatch,
            ordinal,
            name,
            "nullable field violates non-null strict ontology property",
        ));
    }
    Ok(())
}

fn validate_property_name(kind: BulkInputKind, name: &str) -> Result<(), BulkValidationError> {
    validate_identifier_parts(name)
        .then_some(())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::InvalidIdentifier,
                name,
                "property name is not a valid identifier",
            )
        })
}

fn validate_property_type(
    kind: BulkInputKind,
    name: &str,
    data_type: &DataType,
) -> Result<(), BulkValidationError> {
    let supported = match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            property_data_type_supported(field.data_type())
        }
        other => property_data_type_supported(other),
    };
    if supported {
        Ok(())
    } else {
        Err(field_error(
            kind,
            BulkValidationReason::UnsupportedPropertyType,
            name,
            &format!("unsupported Arrow property type {data_type}"),
        ))
    }
}

fn property_data_type_supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            property_data_type_supported(field.data_type())
        }
        _ => matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
        ),
    }
}

fn validate_identifier(
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    value: &str,
) -> Result<(), BulkValidationError> {
    if validate_identifier_parts(value) {
        Ok(())
    } else {
        Err(row_error(
            kind,
            BulkValidationReason::InvalidIdentifier,
            ordinal,
            field,
            "invalid identifier",
        ))
    }
}

fn validate_identifier_parts(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|first| first == '_' || first.is_alphabetic())
        && characters.all(|character| character == '_' || character.is_alphanumeric())
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    kind: BulkInputKind,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, BulkValidationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::SchemaMismatch,
                name,
                "field is not FixedSizeBinary(16)",
            )
        })
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    kind: BulkInputKind,
    name: &str,
) -> Result<&'a StringArray, BulkValidationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::SchemaMismatch,
                name,
                "field is not Utf8",
            )
        })
}

fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    operation_uuid: OperationId,
) -> Result<Uuid, BulkValidationError> {
    validated_uuid_at(values, row, kind, ordinal, field, || {
        Ok(generated_uuid(operation_uuid, kind, ordinal))
    })
}

fn explicit_uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
) -> Result<Uuid, BulkValidationError> {
    validated_uuid_at(values, row, kind, ordinal, field, || {
        Err(uuid_row_error(
            kind,
            ordinal,
            field,
            "endpoint UUID cannot be null",
        ))
    })
}

fn validated_uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    on_null: impl FnOnce() -> Result<Uuid, BulkValidationError>,
) -> Result<Uuid, BulkValidationError> {
    if values.is_null(row) {
        return on_null();
    }
    let uuid = Uuid::from_slice(values.value(row))
        .map_err(|_| uuid_row_error(kind, ordinal, field, "invalid UUID bytes"))?;
    if uuid.get_version_num() != 7 {
        return Err(uuid_row_error(kind, ordinal, field, "value must be UUIDv7"));
    }
    Ok(uuid)
}

fn uuid_row_error(
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    message: &str,
) -> BulkValidationError {
    row_error(
        kind,
        BulkValidationReason::InvalidUuid,
        ordinal,
        field,
        message,
    )
}

fn validate_operation_uuid(
    kind: BulkInputKind,
    operation_uuid: OperationId,
) -> Result<(), BulkValidationError> {
    if operation_uuid.0.get_version_num() == 7 {
        Ok(())
    } else {
        Err(field_error(
            kind,
            BulkValidationReason::InvalidUuid,
            "operation_uuid",
            "operation identity must be UUIDv7",
        ))
    }
}

fn generated_uuid(operation_uuid: OperationId, kind: BulkInputKind, ordinal: u64) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge.bulk.generated-uuid.v1\0");
    hasher.update(operation_uuid.0.as_bytes());
    hasher.update(kind.as_str().as_bytes());
    hasher.update(ordinal.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&operation_uuid.0.as_bytes()[..6]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn open_membership_index(
    graph: &GraphForge,
    input_kind: BulkInputKind,
) -> Result<
    std::sync::MutexGuard<'_, Option<graphforge_storage::UuidMembershipIndex>>,
    BulkValidationError,
> {
    let current_generation =
        graphforge_storage::read_topology_generation(&graph.dir).map_err(|error| {
            contract_error(
                input_kind,
                BulkValidationReason::ProjectState,
                &error.to_string(),
            )
        })?;
    let mut cached = graph.uuid_membership_index.lock().map_err(|error| {
        contract_error(
            input_kind,
            BulkValidationReason::ProjectState,
            &error.to_string(),
        )
    })?;
    if cached
        .as_ref()
        .is_some_and(|index| index.topology_generation() != current_generation)
    {
        *cached = None;
    }
    if !graphforge_storage::uuid_membership_index_present(&graph.dir) {
        let has_nodes = graph.dir.join("topology/nodes.parquet").exists();
        let has_edges = std::fs::read_dir(graph.dir.join("topology/edges"))
            .ok()
            .is_some_and(|mut entries| entries.any(|entry| entry.is_ok()));
        if has_nodes || has_edges {
            return Err(contract_error(
                input_kind,
                BulkValidationReason::ProjectState,
                "UUID membership index is missing; run the bounded storage rebuild before ingest",
            ));
        }
        return Ok(cached);
    }
    if cached.is_none() {
        *cached = Some(
            graphforge_storage::UuidMembershipIndex::open(&graph.dir).map_err(|error| {
                contract_error(
                    input_kind,
                    BulkValidationReason::ProjectState,
                    &error.to_string(),
                )
            })?,
        );
    }
    Ok(cached)
}

fn existing_edge_context(
    graph: &GraphForge,
    endpoint_candidates: &BTreeSet<Uuid>,
    edge_candidates: Option<&BTreeSet<Uuid>>,
) -> Result<(BTreeSet<Uuid>, BTreeSet<Uuid>), BulkValidationError> {
    let mut index = open_membership_index(graph, BulkInputKind::Edge)?;
    let known_nodes = indexed_existing(
        index.as_mut(),
        endpoint_candidates,
        graphforge_storage::UuidIndexKind::Node,
        BulkInputKind::Edge,
    )?;
    let Some(edge_candidates) = edge_candidates else {
        return Ok((known_nodes, BTreeSet::new()));
    };
    let mut existing = indexed_existing(
        index.as_mut(),
        edge_candidates,
        graphforge_storage::UuidIndexKind::Edge,
        BulkInputKind::Edge,
    )?;
    existing.extend(indexed_existing(
        index.as_mut(),
        edge_candidates,
        graphforge_storage::UuidIndexKind::Node,
        BulkInputKind::Edge,
    )?);
    Ok((known_nodes, existing))
}

fn indexed_existing(
    index: Option<&mut graphforge_storage::UuidMembershipIndex>,
    candidates: &BTreeSet<Uuid>,
    index_kind: graphforge_storage::UuidIndexKind,
    input_kind: BulkInputKind,
) -> Result<BTreeSet<Uuid>, BulkValidationError> {
    let Some(index) = index else {
        return Ok(BTreeSet::new());
    };
    let requested = candidates.iter().copied().collect::<Vec<_>>();
    let (found, _) = index.probe(index_kind, &requested).map_err(|error| {
        contract_error(
            input_kind,
            BulkValidationReason::ProjectState,
            &error.to_string(),
        )
    })?;
    Ok(requested
        .into_iter()
        .zip(found)
        .filter_map(|(uuid, present)| present.then_some(uuid))
        .collect())
}

fn candidate_uuids(
    batches: &[RecordBatch],
    kind: BulkInputKind,
    field: &str,
) -> Result<BTreeSet<Uuid>, BulkValidationError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        let uuids = uuid_column(batch, kind, field)?;
        for row in 0..uuids.len() {
            if !uuids.is_null(row) {
                values.insert(Uuid::from_slice(uuids.value(row)).map_err(|error| {
                    contract_error(
                        kind,
                        BulkValidationReason::SchemaMismatch,
                        &error.to_string(),
                    )
                })?);
            }
        }
    }
    Ok(values)
}

fn candidate_endpoint_uuids(
    batches: &[RecordBatch],
) -> Result<BTreeSet<Uuid>, BulkValidationError> {
    let mut values = candidate_uuids(batches, BulkInputKind::Edge, "source_uuid")?;
    values.extend(candidate_uuids(
        batches,
        BulkInputKind::Edge,
        "target_uuid",
    )?);
    Ok(values)
}

#[cfg(test)]
fn indexed_uuid_count(graph: &GraphForge, kind: graphforge_storage::UuidIndexKind) -> u64 {
    graphforge_storage::UuidMembershipIndex::open(&graph.dir)
        .expect("published graph has an authenticated UUID membership index")
        .count(kind)
}

pub(crate) fn register_existing_endpoints(
    writer: &mut graphforge_storage::GraphWriter,
    dir: &std::path::Path,
    endpoints: &BTreeSet<Uuid>,
) -> Result<(), super::GfError> {
    let mut unresolved = endpoints.clone();
    graphforge_storage::visit_nodes_batched(dir, 8_192, |batch| {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "node topology has malformed UUID column".into(),
                )
            })?;
        let ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "node topology has malformed ID column".into(),
                )
            })?;
        for row in 0..batch.num_rows() {
            let uuid = Uuid::from_slice(uuids.value(row)).map_err(|error| {
                datafusion::error::DataFusionError::Execution(error.to_string())
            })?;
            if unresolved.remove(&uuid) {
                writer.register_existing_node(uuid, ids.value(row));
            }
        }
        Ok(!unresolved.is_empty())
    })
    .map_err(|error| super::GfError::Storage(format!("failed to read node topology: {error}")))?;
    if unresolved.is_empty() {
        Ok(())
    } else {
        Err(super::GfError::Validation(
            "bulk edge endpoint disappeared before publication".into(),
        ))
    }
}

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
) -> Result<RecordBatch, super::GfError> {
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
) -> Result<RecordBatch, super::GfError> {
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

fn receipt<'a>(
    rows: impl Iterator<Item = ReceiptRow<'a>>,
    len: usize,
    operation_uuid: OperationId,
    generation_uuid: Uuid,
) -> Result<RecordBatch, super::GfError> {
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
    .map_err(|error| super::GfError::Execution(error.to_string()))
}

fn uuid_array(
    values: impl IntoIterator<Item = Option<Uuid>>,
) -> Result<FixedSizeBinaryArray, super::GfError> {
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        values.into_iter().map(|value| value.map(Uuid::into_bytes)),
        16,
    )
    .map_err(|error| super::GfError::Execution(error.to_string()))
}

fn contract_error(
    kind: BulkInputKind,
    reason: BulkValidationReason,
    message: &str,
) -> BulkValidationError {
    BulkValidationError {
        kind,
        reason,
        batch_index: None,
        row_ordinal: None,
        field: None,
        message: message.to_owned(),
    }
}

fn field_error(
    kind: BulkInputKind,
    reason: BulkValidationReason,
    field: &str,
    message: &str,
) -> BulkValidationError {
    BulkValidationError {
        field: Some(field.to_owned()),
        ..contract_error(kind, reason, message)
    }
}

fn batch_error(
    kind: BulkInputKind,
    batch_index: usize,
    reason: BulkValidationReason,
    field: Option<&str>,
    message: &str,
) -> BulkValidationError {
    BulkValidationError {
        batch_index: Some(batch_index as u64),
        field: field.map(str::to_owned),
        ..contract_error(kind, reason, message)
    }
}

fn row_error(
    kind: BulkInputKind,
    reason: BulkValidationReason,
    ordinal: u64,
    field: &str,
    message: &str,
) -> BulkValidationError {
    BulkValidationError {
        row_ordinal: Some(ordinal),
        field: Some(field.to_owned()),
        ..contract_error(kind, reason, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, StructArray};
    use std::process::Command;

    const FAILPOINT_COOKIE: &str = "graphforge-internal-subprocess-v1";

    fn uuid(seed: u128) -> Uuid {
        let mut bytes = seed.to_be_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn operation(seed: u128) -> OperationId {
        OperationId(uuid(seed))
    }

    #[test]
    fn spatial_bulk_preflight_validates_the_complete_array_before_row_normalization() {
        use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};
        let spatial = SpatialType {
            geometry: SpatialGeometryType::Point,
            crs: SpatialCrs::Epsg4326,
        };
        let field = spatial.field("location", false);
        let array: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(Float64Array::from(vec![-105.0, 181.0])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Float64, false)),
                Arc::new(Float64Array::from(vec![39.7, 0.0])) as ArrayRef,
            ),
        ]));
        let error =
            preflight_spatial_columns(BulkInputKind::Node, 40, &[(&field, &array)]).unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
        assert_eq!(error.row_ordinal, Some(40));
        assert_eq!(error.field.as_deref(), Some("location"));
        assert_eq!(error.message, "GF_SPATIAL_COORDINATE_OUT_OF_RANGE");
    }

    #[test]
    fn wave13_validation_display_and_disappeared_endpoint_are_structured() {
        let error = BulkValidationError {
            kind: BulkInputKind::Edge,
            reason: BulkValidationReason::MissingEndpoint,
            batch_index: Some(2),
            row_ordinal: Some(7),
            field: Some("src_uuid".into()),
            message: "endpoint does not exist".into(),
        };
        assert_eq!(
            error.to_string(),
            "GF_BULK_VALIDATION(missing_endpoint): bulk edge batch 2 row 7 field \"src_uuid\": endpoint does not exist"
        );

        let directory = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        let mut writer = graphforge_storage::GraphWriter::open_at(
            &graph.dir,
            graph.ontology_mode,
            (graph.clock.lock().unwrap())().unwrap(),
        )
        .unwrap();
        let missing = uuid(70_001);
        let failure =
            register_existing_endpoints(&mut writer, &graph.dir, &BTreeSet::from([missing]))
                .unwrap_err();
        assert_eq!(
            failure.to_string(),
            "validation error: bulk edge endpoint disappeared before publication"
        );
    }

    #[test]
    fn strict_ontology_arrow_compatibility_matrix_is_closed() {
        let list_item = Arc::new(Field::new("item", DataType::Utf8, true));
        let compatible = [
            (PropertyValueType::Utf8, DataType::Utf8),
            (PropertyValueType::Utf8, DataType::LargeUtf8),
            (PropertyValueType::Int64, DataType::Int8),
            (PropertyValueType::Int64, DataType::Int16),
            (PropertyValueType::Int64, DataType::Int32),
            (PropertyValueType::Int64, DataType::Int64),
            (PropertyValueType::Int64, DataType::UInt8),
            (PropertyValueType::Int64, DataType::UInt16),
            (PropertyValueType::Int64, DataType::UInt32),
            (PropertyValueType::Float64, DataType::Float32),
            (PropertyValueType::Float64, DataType::Float64),
            (PropertyValueType::Bool, DataType::Boolean),
            (
                PropertyValueType::List,
                DataType::List(Arc::clone(&list_item)),
            ),
            (
                PropertyValueType::List,
                DataType::LargeList(Arc::clone(&list_item)),
            ),
        ];
        for (expected, actual) in compatible {
            validate_ontology_field(
                BulkInputKind::Node,
                7,
                "property",
                &Field::new("property", actual, false),
                &expected,
                false,
            )
            .unwrap();
        }

        for expected in [
            PropertyValueType::Duration,
            PropertyValueType::DateTime,
            PropertyValueType::Map,
        ] {
            let error = validate_ontology_field(
                BulkInputKind::Edge,
                9,
                "property",
                &Field::new("property", DataType::Utf8, false),
                &expected,
                true,
            )
            .unwrap_err();
            assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
            assert_eq!(error.row_ordinal, Some(9));
        }

        let mismatch = validate_ontology_field(
            BulkInputKind::Node,
            11,
            "property",
            &Field::new("property", DataType::Boolean, false),
            &PropertyValueType::Utf8,
            false,
        )
        .unwrap_err();
        assert_eq!(mismatch.reason, BulkValidationReason::PropertyTypeMismatch);
        let nullable = validate_ontology_field(
            BulkInputKind::Node,
            12,
            "property",
            &Field::new("property", DataType::Utf8, true),
            &PropertyValueType::Utf8,
            false,
        )
        .unwrap_err();
        assert_eq!(nullable.reason, BulkValidationReason::NullabilityMismatch);

        for supported in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        ] {
            validate_property_type(BulkInputKind::Node, "property", &supported).unwrap();
        }
        for unsupported in [
            DataType::UInt64,
            DataType::Binary,
            DataType::Date32,
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        ] {
            assert_eq!(
                validate_property_type(BulkInputKind::Edge, "property", &unsupported)
                    .unwrap_err()
                    .reason,
                BulkValidationReason::UnsupportedPropertyType
            );
        }
    }

    #[test]
    fn generated_bulk_identities_are_deterministic_typed_and_domain_separated() {
        let operation_id = operation(42);
        let node_zero = generated_uuid(operation_id, BulkInputKind::Node, 0);
        assert_eq!(
            node_zero,
            generated_uuid(operation_id, BulkInputKind::Node, 0)
        );
        assert_ne!(
            node_zero,
            generated_uuid(operation_id, BulkInputKind::Node, 1)
        );
        assert_ne!(
            node_zero,
            generated_uuid(operation_id, BulkInputKind::Edge, 0)
        );
        assert_eq!(node_zero.get_version_num(), 7);
        assert!(validate_operation_uuid(BulkInputKind::Node, operation_id).is_ok());
        let invalid =
            validate_operation_uuid(BulkInputKind::Edge, OperationId(Uuid::from_u128(42)))
                .unwrap_err();
        assert_eq!(invalid.reason, BulkValidationReason::InvalidUuid);
        assert_eq!(invalid.field.as_deref(), Some("operation_uuid"));

        for valid in ["a", "_a", "alpha_1", "Δelta"] {
            assert!(validate_property_name(BulkInputKind::Node, valid).is_ok());
        }
        for invalid in ["", "1a", "a-b", "a b", "\n"] {
            assert_eq!(
                validate_property_name(BulkInputKind::Edge, invalid)
                    .unwrap_err()
                    .reason,
                BulkValidationReason::InvalidIdentifier
            );
        }

        let graph = GraphForge::new(None).unwrap();
        let stale_nodes = ValidatedBulkNodes {
            rows: Vec::new(),
            operation_uuid: operation(43),
            source_generation_uuid: uuid(44),
        };
        let error = graph
            .validate_bulk_edges(operation(45), &[], &stale_nodes)
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::GenerationMismatch);
        assert_eq!(error.kind, BulkInputKind::Edge);

        let null_uuid = FixedSizeBinaryArray::new_null(16, 1);
        assert_eq!(
            uuid_at(
                &null_uuid,
                0,
                BulkInputKind::Node,
                3,
                "node_uuid",
                operation_id,
            )
            .unwrap(),
            generated_uuid(operation_id, BulkInputKind::Node, 3)
        );
        assert_eq!(
            explicit_uuid_at(&null_uuid, 0, BulkInputKind::Edge, 4, "source_uuid")
                .unwrap_err()
                .reason,
            BulkValidationReason::InvalidUuid
        );
        let v4 = FixedSizeBinaryArray::try_from_iter(
            [Uuid::from_u128(1).as_bytes().as_slice()].into_iter(),
        )
        .unwrap();
        assert_eq!(
            uuid_at(&v4, 0, BulkInputKind::Node, 5, "node_uuid", operation_id,)
                .unwrap_err()
                .reason,
            BulkValidationReason::InvalidUuid
        );
        assert_eq!(
            explicit_uuid_at(&v4, 0, BulkInputKind::Edge, 6, "target_uuid")
                .unwrap_err()
                .reason,
            BulkValidationReason::InvalidUuid
        );
        let short =
            FixedSizeBinaryArray::try_from_iter([b"12345678".as_slice()].into_iter()).unwrap();
        assert_eq!(
            uuid_at(&short, 0, BulkInputKind::Node, 7, "node_uuid", operation_id,)
                .unwrap_err()
                .reason,
            BulkValidationReason::InvalidUuid
        );
    }

    #[test]
    fn public_error_vocabulary_is_complete_and_stable() {
        assert_eq!(BulkInputKind::Node.as_str(), "node");
        assert_eq!(BulkInputKind::Edge.as_str(), "edge");

        let expected = [
            (BulkValidationReason::SchemaMismatch, "schema_mismatch"),
            (BulkValidationReason::ReservedField, "reserved_field"),
            (BulkValidationReason::DuplicateField, "duplicate_field"),
            (
                BulkValidationReason::UnsupportedPropertyType,
                "unsupported_property_type",
            ),
            (
                BulkValidationReason::InvalidIdentifier,
                "invalid_identifier",
            ),
            (BulkValidationReason::InvalidUuid, "invalid_uuid"),
            (BulkValidationReason::IdentityConflict, "identity_conflict"),
            (BulkValidationReason::MissingEndpoint, "missing_endpoint"),
            (
                BulkValidationReason::UnknownOntologyType,
                "unknown_ontology_type",
            ),
            (
                BulkValidationReason::UnknownOntologyProperty,
                "unknown_ontology_property",
            ),
            (
                BulkValidationReason::PropertyTypeMismatch,
                "property_type_mismatch",
            ),
            (
                BulkValidationReason::NullabilityMismatch,
                "nullability_mismatch",
            ),
            (
                BulkValidationReason::GenerationMismatch,
                "generation_mismatch",
            ),
            (BulkValidationReason::ProjectState, "project_state"),
            (BulkValidationReason::OrdinalOverflow, "ordinal_overflow"),
        ];
        for (reason, spelling) in expected {
            assert_eq!(reason.as_str(), spelling);
        }

        let error = BulkValidationError {
            kind: BulkInputKind::Edge,
            reason: BulkValidationReason::MissingEndpoint,
            batch_index: Some(2),
            row_ordinal: Some(7),
            field: Some("source_uuid".into()),
            message: "endpoint does not exist".into(),
        };
        assert_eq!(error.code(), "GF_BULK_VALIDATION");
        assert_eq!(
            error.to_string(),
            "GF_BULK_VALIDATION(missing_endpoint): bulk edge batch 2 row 7 field \"source_uuid\": endpoint does not exist"
        );
    }

    #[test]
    fn property_value_normalization_covers_every_supported_arrow_scalar() {
        let cases: Vec<(ArrayRef, PropValue)> = vec![
            (
                Arc::new(BooleanArray::from(vec![true])),
                PropValue::Bool(true),
            ),
            (Arc::new(Int8Array::from(vec![-8])), PropValue::Int(-8)),
            (Arc::new(Int16Array::from(vec![-16])), PropValue::Int(-16)),
            (Arc::new(Int32Array::from(vec![-32])), PropValue::Int(-32)),
            (Arc::new(Int64Array::from(vec![-64])), PropValue::Int(-64)),
            (Arc::new(UInt8Array::from(vec![8])), PropValue::Int(8)),
            (Arc::new(UInt16Array::from(vec![16])), PropValue::Int(16)),
            (Arc::new(UInt32Array::from(vec![32])), PropValue::Int(32)),
            (
                Arc::new(Float32Array::from(vec![1.5])),
                PropValue::Float(1.5),
            ),
            (
                Arc::new(Float64Array::from(vec![2.5])),
                PropValue::Float(2.5),
            ),
            (
                Arc::new(StringArray::from(vec!["utf8"])),
                PropValue::Str("utf8".into()),
            ),
            (
                Arc::new(LargeStringArray::from(vec!["large-utf8"])),
                PropValue::Str("large-utf8".into()),
            ),
        ];
        for (array, expected) in cases {
            assert_eq!(property_value_at(array.as_ref(), 0).unwrap(), expected);
        }

        let nullable = StringArray::from(vec![None::<&str>]);
        assert_eq!(property_value_at(&nullable, 0).unwrap(), PropValue::Null);

        let unsupported = UInt64Array::from(vec![u64::MAX]);
        assert_eq!(
            property_value_at(&unsupported, 0).unwrap_err(),
            "unsupported property type UInt64"
        );

        let list =
            ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([Some(vec![
                Some(1),
                None,
                Some(3),
            ])]);
        assert_eq!(
            property_value_at(&list, 0).unwrap(),
            PropValue::List(vec![PropValue::Int(1), PropValue::Null, PropValue::Int(3)])
        );
        let large = LargeListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(
            property_value_at(&large, 0).unwrap(),
            PropValue::List(vec![PropValue::Int(4), PropValue::Int(5)])
        );
    }

    #[test]
    fn wave13_property_normalization_and_strict_owner_failures_keep_bulk_context() {
        let field = Field::new("when", DataType::Date32, false);
        let array: ArrayRef = Arc::new(arrow::array::Date32Array::from(vec![1]));
        let columns = [(&field, &array)];
        let unsupported =
            normalize_properties(BulkInputKind::Node, 9, 0, &columns, |_, _| Ok(())).unwrap_err();
        assert_eq!(
            unsupported.reason,
            BulkValidationReason::UnsupportedPropertyType
        );
        assert_eq!(unsupported.row_ordinal, Some(9));
        assert_eq!(unsupported.field.as_deref(), Some("when"));

        let owner_error = normalize_properties(BulkInputKind::Edge, 11, 0, &columns, |name, _| {
            Err(row_error(
                BulkInputKind::Edge,
                BulkValidationReason::UnknownOntologyProperty,
                11,
                name,
                "owner rejected property",
            ))
        })
        .unwrap_err();
        assert_eq!(
            owner_error.reason,
            BulkValidationReason::UnknownOntologyProperty
        );

        let mut graph = GraphForge::new(None).unwrap();
        graph.ontology_mode = OntologyMode::Strict;
        graph.ontology = None;
        for error in [
            validate_node_owner(&graph, 1, "Person").unwrap_err(),
            validate_edge_owner(&graph, 2, "KNOWS").unwrap_err(),
        ] {
            assert_eq!(error.reason, BulkValidationReason::UnknownOntologyType);
        }
        let property_field = Field::new("name", DataType::Utf8, true);
        for error in [
            validate_node_property(&graph, 3, "Person", "name", &property_field).unwrap_err(),
            validate_edge_property(&graph, 4, "KNOWS", "weight", &property_field).unwrap_err(),
        ] {
            assert_eq!(error.reason, BulkValidationReason::ProjectState);
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert_eq!(
            uuid_column(&batch, BulkInputKind::Node, "wrong")
                .unwrap_err()
                .reason,
            BulkValidationReason::SchemaMismatch
        );
        assert_eq!(
            string_column(&batch, BulkInputKind::Edge, "wrong")
                .unwrap_err()
                .reason,
            BulkValidationReason::SchemaMismatch
        );
    }

    fn node_batch(ids: &[Uuid], labels: &[&str], names: &[Option<&str>]) -> RecordBatch {
        let schema =
            bulk_node_input_schema(vec![Field::new("name", DataType::Utf8, true)]).unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap(),
                ),
                Arc::new(StringArray::from(labels.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn edge_batch(
        ids: &[Uuid],
        rel_types: &[&str],
        sources: &[Uuid],
        targets: &[Uuid],
    ) -> RecordBatch {
        let schema = bulk_edge_input_schema(vec![]).unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap(),
                ),
                Arc::new(StringArray::from(rel_types.to_vec())),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(sources.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(targets.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ),
            ],
        )
        .unwrap()
    }

    fn edge_batch_with_weights(
        ids: &[Uuid],
        rel_types: &[&str],
        sources: &[Uuid],
        targets: &[Uuid],
        weights: &[f64],
    ) -> RecordBatch {
        let schema =
            bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)]).unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap(),
                ),
                Arc::new(StringArray::from(rel_types.to_vec())),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(sources.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(targets.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(weights.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schemas_freeze_required_fields_metadata_and_receipt() {
        let nodes = bulk_node_input_schema(vec![
            Field::new("score", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
        .unwrap();
        assert_eq!(
            nodes
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "label", "name", "score"]
        );
        assert_eq!(nodes.metadata()["graphforge.bulk_contract_version"], "1");
        assert!(nodes.field_with_name("node_uuid").unwrap().is_nullable());
        let receipt = bulk_receipt_schema();
        assert_eq!(
            receipt
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "row_ordinal",
                "entity_kind",
                "entity_uuid",
                "label",
                "rel_type",
                "source_uuid",
                "target_uuid",
                "operation_uuid",
                "publication_generation_uuid"
            ]
        );
        assert!(
            receipt
                .field_with_name("source_uuid")
                .unwrap()
                .is_nullable()
        );
        assert!(
            bulk_node_input_schema(vec![Field::new("node_uuid", DataType::Utf8, false)]).is_err()
        );

        let edges =
            bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, true)]).unwrap();
        assert_eq!(
            edges
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "edge_uuid",
                "rel_type",
                "source_uuid",
                "target_uuid",
                "weight"
            ]
        );
        assert_eq!(edges.metadata()["graphforge.bulk_kind"], "edge");
        assert!(edges.field_with_name("edge_uuid").unwrap().is_nullable());
        assert!(!edges.field_with_name("source_uuid").unwrap().is_nullable());
    }

    #[test]
    fn node_and_edge_receipts_populate_only_kind_applicable_columns() {
        let operation_uuid = operation(889);
        let generation_uuid = uuid(888);
        let node_uuid = uuid(887);
        let node = node_receipt(
            &[BulkNodeRow {
                row_ordinal: 4,
                node_uuid,
                label: "Host".into(),
                properties: BTreeMap::new(),
            }],
            operation_uuid,
            generation_uuid,
        )
        .unwrap();
        assert_eq!(
            node.column_by_name("row_ordinal")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            4
        );
        assert_eq!(
            node.column_by_name("entity_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "node"
        );
        assert_eq!(
            node.column_by_name("label")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Host"
        );
        for name in ["rel_type", "source_uuid", "target_uuid"] {
            assert!(node.column_by_name(name).unwrap().is_null(0), "{name}");
        }

        let edge_uuid = uuid(886);
        let source_uuid = uuid(885);
        let target_uuid = uuid(884);
        let edge = edge_receipt(
            &[BulkEdgeRow {
                row_ordinal: 7,
                edge_uuid,
                rel_type: "CONNECTS".into(),
                source_uuid,
                target_uuid,
                properties: BTreeMap::new(),
            }],
            operation_uuid,
            generation_uuid,
        )
        .unwrap();
        assert_eq!(
            edge.column_by_name("entity_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge"
        );
        assert!(edge.column_by_name("label").unwrap().is_null(0));
        assert_eq!(
            edge.column_by_name("rel_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "CONNECTS"
        );
        for (name, expected) in [
            ("entity_uuid", edge_uuid),
            ("source_uuid", source_uuid),
            ("target_uuid", target_uuid),
            ("operation_uuid", operation_uuid.0),
            ("publication_generation_uuid", generation_uuid),
        ] {
            let values = edge
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(
                Uuid::from_slice(values.value(0)).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn canonical_schema_order_and_metadata_are_enforced() {
        let graph = GraphForge::new(None).unwrap();
        let missing_metadata = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("label", DataType::Utf8, false),
        ]));
        let empty_uuid = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::empty::<Option<[u8; 16]>>(),
            16,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            missing_metadata,
            vec![
                Arc::new(empty_uuid),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
        )
        .unwrap();
        let error = graph
            .validate_bulk_nodes(operation(890), &[batch])
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
        assert_eq!(error.batch_index, Some(0));

        for metadata in [
            contract_metadata("edge"),
            HashMap::from([
                (
                    "graphforge.bulk_contract_version".to_owned(),
                    "2".to_owned(),
                ),
                ("graphforge.bulk_kind".to_owned(), "node".to_owned()),
                (
                    "graphforge.row_order".to_owned(),
                    "logical_input_order".to_owned(),
                ),
            ]),
        ] {
            let wrong_metadata = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                    Field::new("label", DataType::Utf8, false),
                ],
                metadata,
            ));
            let error = graph
                .validate_bulk_nodes(operation(890), &[RecordBatch::new_empty(wrong_metadata)])
                .unwrap_err();
            assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
        }

        let unsorted = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("label", DataType::Utf8, false),
                Field::new("zeta", DataType::Utf8, true),
                Field::new("alpha", DataType::Utf8, true),
            ],
            contract_metadata("node"),
        ));
        let batch = RecordBatch::new_empty(unsorted);
        let error = graph
            .validate_bulk_nodes(operation(891), &[batch])
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
        assert!(error.message.contains("lexicographically ordered"));

        let unsupported_list = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "events",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        true,
                    ))),
                    true,
                ),
            ],
            contract_metadata("node"),
        ));
        let error = graph
            .validate_bulk_nodes(operation(891), &[RecordBatch::new_empty(unsupported_list)])
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::UnsupportedPropertyType);
        assert_eq!(error.field.as_deref(), Some("events"));
    }

    #[test]
    fn wave13_public_schema_validation_matrix_preserves_error_kind_field_and_partition() {
        let reserved =
            bulk_node_input_schema(vec![Field::new("label", DataType::Utf8, true)]).unwrap_err();
        assert_eq!(reserved.reason, BulkValidationReason::ReservedField);
        assert_eq!(reserved.field.as_deref(), Some("label"));

        let duplicate = bulk_edge_input_schema(vec![
            Field::new("weight", DataType::Float64, true),
            Field::new("weight", DataType::Float64, false),
        ])
        .unwrap_err();
        assert_eq!(duplicate.reason, BulkValidationReason::DuplicateField);
        assert_eq!(duplicate.field.as_deref(), Some("weight"));

        let invalid = bulk_node_input_schema(vec![Field::new("not valid", DataType::Utf8, true)])
            .unwrap_err();
        assert_eq!(invalid.reason, BulkValidationReason::InvalidIdentifier);
        assert_eq!(invalid.field.as_deref(), Some("not valid"));

        let unsupported =
            bulk_edge_input_schema(vec![Field::new("counter", DataType::UInt64, false)])
                .unwrap_err();
        assert_eq!(
            unsupported.reason,
            BulkValidationReason::UnsupportedPropertyType
        );
        assert_eq!(unsupported.field.as_deref(), Some("counter"));

        let graph = GraphForge::new(None).unwrap();
        let missing = Arc::new(Schema::new_with_metadata(
            vec![Field::new("node_uuid", DataType::FixedSizeBinary(16), true)],
            contract_metadata("node"),
        ));
        let missing = graph
            .validate_bulk_nodes(operation(892), &[RecordBatch::new_empty(missing)])
            .unwrap_err();
        assert_eq!(missing.reason, BulkValidationReason::SchemaMismatch);
        assert_eq!(missing.batch_index, Some(0));
        assert!(missing.message.contains("required topology fields"));

        let first = RecordBatch::new_empty(bulk_node_input_schema(vec![]).unwrap());
        let second = RecordBatch::new_empty(
            bulk_node_input_schema(vec![Field::new("name", DataType::Utf8, true)]).unwrap(),
        );
        let drift = graph
            .validate_bulk_nodes(operation(893), &[first, second])
            .unwrap_err();
        assert_eq!(drift.reason, BulkValidationReason::SchemaMismatch);
        assert_eq!(drift.batch_index, Some(1));
        assert_eq!(drift.message, "schema differs from batch 0");

        let wrong_edge = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("rel_type", DataType::LargeUtf8, false),
                Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
            ],
            contract_metadata("edge"),
        ));
        let source_generation_uuid = *graph.current_generation_uuid.lock().unwrap();
        let wrong_edge = graph
            .validate_bulk_edges(
                operation(894),
                &[RecordBatch::new_empty(wrong_edge)],
                &ValidatedBulkNodes {
                    rows: vec![],
                    operation_uuid: operation(894),
                    source_generation_uuid,
                },
            )
            .unwrap_err();
        assert_eq!(wrong_edge.reason, BulkValidationReason::SchemaMismatch);
        assert_eq!(wrong_edge.batch_index, Some(0));
        assert_eq!(wrong_edge.field.as_deref(), Some("rel_type"));
    }

    #[test]
    fn property_and_ontology_type_registries_cover_every_supported_family() {
        let scalar_types = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
        ];
        for data_type in scalar_types {
            assert!(validate_property_type(BulkInputKind::Node, "value", &data_type).is_ok());
        }
        for data_type in [
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        ] {
            assert!(validate_property_type(BulkInputKind::Edge, "values", &data_type).is_ok());
        }
        let error =
            validate_property_type(BulkInputKind::Node, "when", &DataType::Date32).unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::UnsupportedPropertyType);
        assert_eq!(error.field.as_deref(), Some("when"));

        for identifier in ["Person", "_private", "Ångström2"] {
            assert!(validate_identifier(BulkInputKind::Node, 0, "label", identifier).is_ok());
            assert!(validate_property_name(BulkInputKind::Node, identifier).is_ok());
        }
        for identifier in ["", "9name", "has space", "has-dash"] {
            let row =
                validate_identifier(BulkInputKind::Edge, 7, "rel_type", identifier).unwrap_err();
            assert_eq!(row.reason, BulkValidationReason::InvalidIdentifier);
            assert_eq!(row.row_ordinal, Some(7));
            let field = validate_property_name(BulkInputKind::Edge, identifier).unwrap_err();
            assert_eq!(field.reason, BulkValidationReason::InvalidIdentifier);
            assert_eq!(field.field.as_deref(), Some(identifier));
        }

        let compatible = [
            (PropertyValueType::Utf8, DataType::LargeUtf8),
            (PropertyValueType::Int64, DataType::UInt32),
            (PropertyValueType::Float64, DataType::Float32),
            (PropertyValueType::Bool, DataType::Boolean),
            (
                PropertyValueType::List,
                DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
            ),
        ];
        for (expected, actual) in compatible {
            assert!(
                validate_ontology_field(
                    BulkInputKind::Node,
                    3,
                    "value",
                    &Field::new("value", actual, false),
                    &expected,
                    false,
                )
                .is_ok()
            );
        }
        for expected in [
            PropertyValueType::Duration,
            PropertyValueType::DateTime,
            PropertyValueType::Map,
        ] {
            let error = validate_ontology_field(
                BulkInputKind::Node,
                3,
                "value",
                &Field::new("value", DataType::Utf8, false),
                &expected,
                false,
            )
            .unwrap_err();
            assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
        }
        let error = validate_ontology_field(
            BulkInputKind::Edge,
            4,
            "weight",
            &Field::new("weight", DataType::Float64, true),
            &PropertyValueType::Float64,
            false,
        )
        .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::NullabilityMismatch);
        assert_eq!(error.row_ordinal, Some(4));
    }

    #[test]
    fn null_identity_generation_is_operation_and_ordinal_deterministic() {
        let graph = GraphForge::new(None).unwrap();
        let schema = bulk_node_input_schema(vec![]).unwrap();
        let generated = |labels: Vec<&str>| {
            let row_count = labels.len();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            std::iter::repeat(None::<[u8; 16]>).take(row_count),
                            16,
                        )
                        .unwrap(),
                    ),
                    Arc::new(StringArray::from(labels)),
                ],
            )
            .unwrap()
        };
        let first = graph
            .validate_bulk_nodes(operation(892), &[generated(vec!["Person", "Person"])])
            .unwrap();
        let second = graph
            .validate_bulk_nodes(
                operation(892),
                &[generated(vec!["Person"]), generated(vec!["Person"])],
            )
            .unwrap();
        let different_operation = graph
            .validate_bulk_nodes(operation(893), &[generated(vec!["Person", "Person"])])
            .unwrap();
        assert_eq!(first.rows(), second.rows());
        assert_ne!(
            first.rows()[0].node_uuid,
            different_operation.rows()[0].node_uuid
        );
        assert_ne!(first.rows()[0].node_uuid, first.rows()[1].node_uuid);
        assert!(
            first
                .rows()
                .iter()
                .all(|row| row.node_uuid.get_version_num() == 7)
        );

        let source = uuid(894);
        let target = uuid(895);
        let nodes = graph
            .validate_bulk_nodes(
                operation(894),
                &[node_batch(
                    &[source, target],
                    &["Person", "Person"],
                    &[None, None],
                )],
            )
            .unwrap();
        let edge = RecordBatch::try_new(
            bulk_edge_input_schema(vec![]).unwrap(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [None::<[u8; 16]>].into_iter(),
                        16,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["KNOWS"])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([source.as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([target.as_bytes()].into_iter()).unwrap(),
                ),
            ],
        )
        .unwrap();
        let edge = graph
            .validate_bulk_edges(operation(892), &[edge], &nodes)
            .unwrap();
        assert_ne!(
            first.rows()[0].node_uuid,
            edge.rows()[0].edge_uuid,
            "validated node and edge generation domains must not collide"
        );
    }

    #[test]
    fn empty_and_partitioned_nodes_preserve_logical_ordinals_and_values() {
        let graph = GraphForge::new(None).unwrap();
        assert!(
            graph
                .validate_bulk_nodes(operation(900), &[])
                .unwrap()
                .rows()
                .is_empty()
        );
        let first = node_batch(&[uuid(1)], &["Person"], &[Some("Alice")]);
        let second = node_batch(
            &[uuid(2), uuid(3)],
            &["Person", "Person"],
            &[None, Some("Cara")],
        );
        let validated = graph
            .validate_bulk_nodes(operation(900), &[first, second])
            .unwrap();
        assert_eq!(
            validated.source_generation_uuid(),
            *graph.current_generation_uuid.lock().unwrap()
        );
        assert_eq!(validated.operation_uuid(), operation(900));
        assert_eq!(
            validated
                .rows()
                .iter()
                .map(|row| row.row_ordinal)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(
            validated.rows()[0].properties["name"],
            PropValue::Str("Alice".into())
        );
        assert_eq!(validated.rows()[1].properties["name"], PropValue::Null);
    }

    #[test]
    fn deterministic_first_error_precedes_later_row_and_property_defects() {
        let graph = GraphForge::new(None).unwrap();
        let duplicate = uuid(9);
        let batch = node_batch(
            &[duplicate, duplicate],
            &["Person", "bad-label"],
            &[Some("ok"), Some("later")],
        );
        let error = graph
            .validate_bulk_nodes(operation(901), &[batch])
            .unwrap_err();
        assert_eq!(error.code(), "GF_BULK_VALIDATION");
        assert_eq!(error.kind, BulkInputKind::Node);
        assert_eq!(error.reason, BulkValidationReason::IdentityConflict);
        assert_eq!(error.row_ordinal, Some(1));
        assert_eq!(error.field.as_deref(), Some("node_uuid"));

        let first = node_batch(&[duplicate], &["Person"], &[Some("ok")]);
        let second = node_batch(&[duplicate], &["bad-label"], &[Some("later")]);
        assert_eq!(
            graph
                .validate_bulk_nodes(operation(901), &[first, second])
                .unwrap_err()
                .to_string(),
            error.to_string(),
            "logical error order must not depend on record-batch partitioning"
        );
    }

    #[test]
    fn malformed_schema_and_non_v7_uuid_fail_with_field_context() {
        let graph = GraphForge::new(None).unwrap();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::Utf8, true),
                Field::new("label", DataType::Utf8, false),
            ],
            contract_metadata("node"),
        ));
        let malformed = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["not-a-uuid"])),
                Arc::new(StringArray::from(vec!["Person"])),
            ],
        )
        .unwrap();
        let malformed = graph
            .validate_bulk_nodes(operation(902), &[malformed])
            .unwrap_err();
        assert_eq!(malformed.reason, BulkValidationReason::SchemaMismatch);
        assert_eq!(malformed.field.as_deref(), Some("node_uuid"));

        let non_v7 = Uuid::from_u128(4);
        let error = graph
            .validate_bulk_nodes(
                operation(902),
                &[node_batch(&[non_v7], &["Person"], &[None])],
            )
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::InvalidUuid);
        assert_eq!(error.field.as_deref(), Some("node_uuid"));
    }

    #[test]
    fn edge_endpoints_accept_same_request_nodes_and_reject_missing_nodes() {
        let graph = GraphForge::new(None).unwrap();
        let source = uuid(20);
        let target = uuid(21);
        let nodes = graph
            .validate_bulk_nodes(
                operation(903),
                &[node_batch(
                    &[source, target],
                    &["Person", "Person"],
                    &[None, None],
                )],
            )
            .unwrap();
        let valid = edge_batch(&[uuid(22)], &["KNOWS"], &[source], &[target]);
        let validated_edges = graph
            .validate_bulk_edges(operation(904), &[valid], &nodes)
            .unwrap();
        assert_eq!(validated_edges.rows().len(), 1);
        assert_eq!(
            validated_edges.source_generation_uuid(),
            nodes.source_generation_uuid()
        );
        assert_eq!(validated_edges.operation_uuid(), operation(904));

        let missing = edge_batch(&[uuid(23)], &["KNOWS"], &[source], &[uuid(99)]);
        let error = graph
            .validate_bulk_edges(operation(905), &[missing], &nodes)
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::MissingEndpoint);
        assert_eq!(error.row_ordinal, Some(0));
        assert_eq!(error.field.as_deref(), Some("target_uuid"));

        let duplicate = uuid(24);
        let duplicate_edges = edge_batch(
            &[duplicate, duplicate],
            &["KNOWS", "KNOWS"],
            &[source, source],
            &[target, target],
        );
        let duplicate = graph
            .validate_bulk_edges(operation(906), &[duplicate_edges], &nodes)
            .unwrap_err();
        assert_eq!(duplicate.reason, BulkValidationReason::IdentityConflict);
        assert_eq!(duplicate.row_ordinal, Some(1));

        let cross_kind = edge_batch(&[source], &["KNOWS"], &[source], &[target]);
        let cross_kind = graph
            .validate_bulk_edges(operation(907), &[cross_kind], &nodes)
            .unwrap_err();
        assert_eq!(cross_kind.reason, BulkValidationReason::IdentityConflict);
        assert_eq!(cross_kind.row_ordinal, Some(0));
    }

    #[test]
    fn validation_is_zero_write_for_catalog_generation_and_graph_bytes() {
        let graph = GraphForge::new(None).unwrap();
        let before = crate::graph_snapshot::capture(&graph.dir).unwrap();
        let catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let invalid = node_batch(&[uuid(30), uuid(30)], &["Person", "Person"], &[None, None]);
        assert!(
            graph
                .validate_bulk_nodes(operation(908), &[invalid])
                .is_err()
        );
        assert_eq!(
            crate::graph_snapshot::capture(&graph.dir).unwrap().bytes,
            before.bytes
        );
        assert_eq!(
            graph.runtime_catalog.lock().unwrap().to_record_batch(),
            catalog
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);

        let valid = node_batch(&[uuid(31)], &["Person"], &[Some("Alice")]);
        assert_eq!(
            graph
                .validate_bulk_nodes(operation(909), &[valid])
                .unwrap()
                .rows()
                .len(),
            1
        );
        assert_eq!(
            crate::graph_snapshot::capture(&graph.dir).unwrap().bytes,
            before.bytes
        );
        assert_eq!(
            graph.runtime_catalog.lock().unwrap().to_record_batch(),
            catalog
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
    }

    #[test]
    fn wave13_strict_inherited_properties_and_types_are_validated_without_publication() {
        let dir = tempfile::TempDir::new().unwrap();
        let project_path = dir.path().join("project");
        std::fs::create_dir(&project_path).unwrap();
        let ontology_path = dir.path().join("strict.yaml");
        std::fs::write(
            &ontology_path,
            "ontology_id: bulk\nversion: \"1\"\nentity_types:\n  - name: Asset\n    abstract: false\n  - name: Host\n    abstract: false\n    parent: Asset\nrelation_types:\n  - name: CONNECTS\n    src: Host\n    dst: Host\nproperties:\n  - owner: Asset\n    name: name\n    type: utf8\n    nullable: true\n  - owner: Host\n    name: score\n    type: int64\n    nullable: false\n  - owner: CONNECTS\n    name: weight\n    type: float64\n    nullable: false\n",
        )
        .unwrap();
        let mut graph = GraphForge::new(project_path.to_str()).unwrap();
        graph
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(uuid(40)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: OntologyMode::Strict,
            })
            .unwrap();

        let unknown_owner = graph
            .validate_bulk_nodes(
                operation(909),
                &[node_batch(&[uuid(409)], &["Unknown"], &[None])],
            )
            .unwrap_err();
        assert_eq!(
            unknown_owner.reason,
            BulkValidationReason::UnknownOntologyType
        );
        assert_eq!(unknown_owner.row_ordinal, Some(0));
        assert_eq!(unknown_owner.field.as_deref(), Some("label"));
        assert_eq!(unknown_owner.message, "unknown strict ontology entity type");

        let unknown_property_schema =
            bulk_node_input_schema(vec![Field::new("alias", DataType::Utf8, true)]).unwrap();
        let unknown_property_batch = RecordBatch::try_new(
            unknown_property_schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(410).as_bytes()].into_iter())
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Host"])),
                Arc::new(StringArray::from(vec![Some("gateway")])),
            ],
        )
        .unwrap();
        let unknown_property = graph
            .validate_bulk_nodes(operation(909), &[unknown_property_batch])
            .unwrap_err();
        assert_eq!(
            unknown_property.reason,
            BulkValidationReason::UnknownOntologyProperty
        );
        assert_eq!(unknown_property.row_ordinal, Some(0));
        assert_eq!(unknown_property.field.as_deref(), Some("alias"));
        assert_eq!(
            unknown_property.message,
            "property is not declared for strict entity type"
        );

        let schema = bulk_node_input_schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, false),
        ])
        .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Host"])),
                Arc::new(StringArray::from(vec![Some("gateway")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        assert_eq!(
            graph
                .validate_bulk_nodes(operation(910), &[batch])
                .unwrap()
                .rows()
                .len(),
            1
        );

        let before_graph = crate::graph_snapshot::capture(&graph.dir).unwrap();
        let before_catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
        let before_generation = *graph.current_generation_uuid.lock().unwrap();
        let before_ontology = graph
            .workspace_ontology()
            .unwrap()
            .to_canonical_json()
            .unwrap();
        let before_configuration = graph
            .workspace_configuration()
            .unwrap()
            .to_canonical_json()
            .unwrap();

        let wrong =
            bulk_node_input_schema(vec![Field::new("score", DataType::Utf8, false)]).unwrap();
        let batch = RecordBatch::try_new(
            wrong,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(42).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Host"])),
                Arc::new(StringArray::from(vec!["seven"])),
            ],
        )
        .unwrap();
        let error = graph
            .validate_bulk_nodes(operation(911), &[batch])
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
        assert_eq!(error.field.as_deref(), Some("score"));
        assert_eq!(
            crate::graph_snapshot::capture(&graph.dir).unwrap().bytes,
            before_graph.bytes
        );
        assert_eq!(
            graph.runtime_catalog.lock().unwrap().to_record_batch(),
            before_catalog
        );
        assert_eq!(
            *graph.current_generation_uuid.lock().unwrap(),
            before_generation
        );
        assert_eq!(
            graph
                .workspace_ontology()
                .unwrap()
                .to_canonical_json()
                .unwrap(),
            before_ontology
        );
        assert_eq!(
            graph
                .workspace_configuration()
                .unwrap()
                .to_canonical_json()
                .unwrap(),
            before_configuration
        );

        let nullable_score =
            bulk_node_input_schema(vec![Field::new("score", DataType::Int64, true)]).unwrap();
        let nullable_score = RecordBatch::try_new(
            nullable_score,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(43).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Host"])),
                Arc::new(Int64Array::from(vec![Some(7)])),
            ],
        )
        .unwrap();
        assert_eq!(
            graph
                .validate_bulk_nodes(operation(912), &[nullable_score])
                .unwrap_err()
                .reason,
            BulkValidationReason::NullabilityMismatch
        );

        let edge_schema =
            bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)]).unwrap();
        let edge = RecordBatch::try_new(
            edge_schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(45).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec!["CONNECTS"])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(arrow::array::Float64Array::from(vec![0.5])),
            ],
        )
        .unwrap();
        let same_request = ValidatedBulkNodes {
            rows: vec![BulkNodeRow {
                row_ordinal: 0,
                node_uuid: uuid(41),
                label: "Host".into(),
                properties: BTreeMap::new(),
            }],
            operation_uuid: operation(913),
            source_generation_uuid: *graph.current_generation_uuid.lock().unwrap(),
        };

        let unknown_edge_property_schema =
            bulk_edge_input_schema(vec![Field::new("alias", DataType::Utf8, true)]).unwrap();
        let unknown_edge_property = RecordBatch::try_new(
            unknown_edge_property_schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(44).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec!["CONNECTS"])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
                ),
                Arc::new(StringArray::from(vec![Some("uplink")])),
            ],
        )
        .unwrap();
        let unknown_edge_property = graph
            .validate_bulk_edges(operation(913), &[unknown_edge_property], &same_request)
            .unwrap_err();
        assert_eq!(
            unknown_edge_property.reason,
            BulkValidationReason::UnknownOntologyProperty
        );
        assert_eq!(unknown_edge_property.row_ordinal, Some(0));
        assert_eq!(unknown_edge_property.field.as_deref(), Some("alias"));
        assert_eq!(
            unknown_edge_property.message,
            "property is not declared for strict relationship type"
        );

        assert_eq!(
            graph
                .validate_bulk_edges(operation(913), &[edge], &same_request)
                .unwrap()
                .rows()
                .len(),
            1
        );

        let strict_node_schema = bulk_node_input_schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, false),
        ])
        .unwrap();
        let strict_nodes = RecordBatch::try_new(
            strict_node_schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        [uuid(46), uuid(47)].iter().map(Uuid::as_bytes),
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Host", "Host"])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        graph
            .publish_bulk_nodes(operation(914), &[strict_nodes])
            .unwrap();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let invalid_relation = edge_batch(&[uuid(48)], &["UNKNOWN"], &[uuid(46)], &[uuid(47)]);
        let error = graph
            .publish_bulk_edges(operation(915), &[invalid_relation])
            .unwrap_err();
        assert!(matches!(
            error,
            BulkEdgePublicationError::Validation(BulkValidationError {
                reason: BulkValidationReason::UnknownOntologyType,
                ..
            })
        ));
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
        assert_eq!(
            indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
            0
        );
    }

    #[test]
    fn publish_bulk_nodes_empty_is_a_zero_row_noop() {
        let graph = GraphForge::new(None).unwrap();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let receipt = graph.publish_bulk_nodes(operation(920), &[]).unwrap();
        assert_eq!(receipt.num_rows(), 0);
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
    }

    #[test]
    fn publish_bulk_nodes_is_atomic_ordered_and_idempotent_after_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("project");
        std::fs::create_dir(&path).unwrap();
        let batch = node_batch(
            &[uuid(921), uuid(922)],
            &["Person", "Person"],
            &[Some("Ada"), Some("Grace")],
        );
        let graph = GraphForge::new(path.to_str()).unwrap();
        let receipt = graph
            .publish_bulk_nodes(operation(923), std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(receipt.num_rows(), 2);
        let generation = *graph.current_generation_uuid.lock().unwrap();
        drop(graph);

        let reopened = GraphForge::new(path.to_str()).unwrap();
        let replay = reopened
            .publish_bulk_nodes(operation(923), std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(replay, receipt);
        assert_eq!(
            *reopened.current_generation_uuid.lock().unwrap(),
            generation
        );
        assert_eq!(
            indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Node),
            2
        );

        let changed = node_batch(&[uuid(924)], &["Person"], &[Some("Changed")]);
        let error = reopened
            .publish_bulk_nodes(operation(923), &[changed])
            .unwrap_err();
        assert!(matches!(
            error,
            BulkNodePublicationError::Publication(super::super::GfError::Project {
                code: graphforge_core::ProjectErrorCode::TransactionConflict,
                ..
            })
        ));
        assert_eq!(
            indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Node),
            2
        );
    }

    #[test]
    fn publish_bulk_edges_empty_is_a_zero_row_noop() {
        let graph = GraphForge::new(None).unwrap();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let receipt = graph.publish_bulk_edges(operation(930), &[]).unwrap();
        assert_eq!(receipt.num_rows(), 0);
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
    }

    #[test]
    fn publish_bulk_edges_is_one_generation_ordered_and_idempotent_after_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("project");
        std::fs::create_dir(&path).unwrap();
        let graph = GraphForge::new(path.to_str()).unwrap();
        let node_ids = (0..32).map(|index| uuid(1_000 + index)).collect::<Vec<_>>();
        let labels = vec!["Person"; node_ids.len()];
        let names = vec![None; node_ids.len()];
        graph
            .publish_bulk_nodes(operation(931), &[node_batch(&node_ids, &labels, &names)])
            .unwrap();
        let edge_ids = (0..256)
            .map(|index| uuid(2_000 + index))
            .collect::<Vec<_>>();
        let rel_types = vec!["KNOWS"; edge_ids.len()];
        let sources = (0..edge_ids.len())
            .map(|index| node_ids[index % node_ids.len()])
            .collect::<Vec<_>>();
        let targets = (0..edge_ids.len())
            .map(|index| node_ids[(index + 1) % node_ids.len()])
            .collect::<Vec<_>>();
        let weights = (0..edge_ids.len())
            .map(|index| index as f64 / 10.0)
            .collect::<Vec<_>>();
        let batch = edge_batch_with_weights(&edge_ids, &rel_types, &sources, &targets, &weights);
        let generation_count = std::fs::read_dir(path.join("generations")).unwrap().count();
        let transaction_count = std::fs::read_dir(path.join("transactions"))
            .unwrap()
            .count();

        let receipt = graph
            .publish_bulk_edges(operation(932), std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(receipt.num_rows(), 256);
        let receipt_ids = receipt
            .column_by_name("entity_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(
            receipt_ids
                .iter()
                .zip(&edge_ids)
                .all(|(actual, expected)| actual == Some(expected.as_bytes().as_slice()))
        );
        assert_eq!(
            std::fs::read_dir(path.join("generations")).unwrap().count(),
            generation_count + 1
        );
        assert_eq!(
            std::fs::read_dir(path.join("transactions"))
                .unwrap()
                .count(),
            transaction_count + 1
        );
        let generation = *graph.current_generation_uuid.lock().unwrap();
        drop(graph);

        let reopened = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Edge),
            256
        );
        assert_eq!(
            reopened
                .execute("MATCH ()-[r:KNOWS]->() RETURN r.weight")
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            256
        );
        let node_collision =
            edge_batch(&[node_ids[31]], &["KNOWS"], &[node_ids[0]], &[node_ids[1]]);
        let collision = reopened
            .publish_bulk_edges(operation(934), &[node_collision])
            .unwrap_err();
        assert!(matches!(
            collision,
            BulkEdgePublicationError::Validation(BulkValidationError {
                kind: BulkInputKind::Edge,
                reason: BulkValidationReason::IdentityConflict,
                ..
            })
        ));
        let replay = reopened
            .publish_bulk_edges(operation(932), std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(replay, receipt);
        assert_eq!(
            *reopened.current_generation_uuid.lock().unwrap(),
            generation
        );
        assert_eq!(
            std::fs::read_dir(path.join("generations")).unwrap().count(),
            generation_count + 1
        );

        let changed = edge_batch(&[uuid(3_000)], &["LIKES"], &[node_ids[0]], &[node_ids[1]]);
        let error = reopened
            .publish_bulk_edges(operation(932), &[changed])
            .unwrap_err();
        assert!(matches!(
            error,
            BulkEdgePublicationError::Publication(super::super::GfError::Project {
                code: graphforge_core::ProjectErrorCode::TransactionConflict,
                ..
            })
        ));
        assert_eq!(
            indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Edge),
            256
        );
    }

    #[test]
    fn publish_bulk_edges_rejects_missing_endpoint_without_a_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("project");
        std::fs::create_dir(&path).unwrap();
        let graph = GraphForge::new(path.to_str()).unwrap();
        let nodes = [uuid(4_000), uuid(4_001)];
        graph
            .publish_bulk_nodes(
                operation(933),
                &[node_batch(&nodes, &["Person", "Person"], &[None, None])],
            )
            .unwrap();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let generation_count = std::fs::read_dir(path.join("generations")).unwrap().count();
        let batch = edge_batch(
            &[uuid(4_002), uuid(4_003)],
            &["KNOWS", "KNOWS"],
            &[nodes[0], nodes[0]],
            &[nodes[1], uuid(9_999)],
        );
        let error = graph
            .publish_bulk_edges(operation(934), &[batch])
            .unwrap_err();
        assert!(matches!(
            error,
            BulkEdgePublicationError::Validation(BulkValidationError {
                reason: BulkValidationReason::MissingEndpoint,
                row_ordinal: Some(1),
                ..
            })
        ));
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
        assert_eq!(
            indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
            0
        );
        assert_eq!(
            std::fs::read_dir(path.join("generations")).unwrap().count(),
            generation_count
        );

        let duplicate = edge_batch(
            &[uuid(4_004), uuid(4_004)],
            &["KNOWS", "KNOWS"],
            &[nodes[0], nodes[1]],
            &[nodes[1], nodes[0]],
        );
        let error = graph
            .publish_bulk_edges(operation(935), &[duplicate])
            .unwrap_err();
        assert!(matches!(
            error,
            BulkEdgePublicationError::Validation(BulkValidationError {
                reason: BulkValidationReason::IdentityConflict,
                row_ordinal: Some(1),
                ..
            })
        ));
        assert_eq!(
            indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
            0
        );
        assert_eq!(
            std::fs::read_dir(path.join("generations")).unwrap().count(),
            generation_count
        );
    }

    #[test]
    fn wave13_bulk_publication_conflicts_preserve_the_committed_generation_and_rows() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        let left = uuid(9_100);
        let right = uuid(9_101);
        let node_operation = operation(9_102);
        let original = node_batch(
            &[left, right],
            &["Person", "Person"],
            &[Some("A"), Some("B")],
        );
        graph
            .publish_bulk_nodes(node_operation, &[original.clone()])
            .unwrap();
        let committed = graphforge_storage::resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid();

        let changed = node_batch(
            &[left, right],
            &["Person", "Person"],
            &[Some("A"), Some("changed")],
        );
        let error = graph
            .publish_bulk_nodes(node_operation, &[changed])
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "GF_IDEMPOTENCY_CONFLICT: bulk-node operation UUID was already used with different input"
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(directory.path())
                .unwrap()
                .generation_uuid(),
            committed
        );
        let collision = graph
            .validate_bulk_nodes(operation(9_103), &[original])
            .unwrap_err();
        assert_eq!(collision.reason, BulkValidationReason::IdentityConflict);
        assert_eq!(collision.row_ordinal, Some(0));

        let edge = uuid(9_104);
        let edge_operation = operation(9_105);
        let original_edge = edge_batch(&[edge], &["KNOWS"], &[left], &[right]);
        graph
            .publish_bulk_edges(edge_operation, &[original_edge.clone()])
            .unwrap();
        let edge_committed = graphforge_storage::resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid();
        let changed_edge = edge_batch(&[edge], &["LIKES"], &[left], &[right]);
        let error = graph
            .publish_bulk_edges(edge_operation, &[changed_edge])
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "GF_IDEMPOTENCY_CONFLICT: bulk-edge operation UUID was already used with different input"
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(directory.path())
                .unwrap()
                .generation_uuid(),
            edge_committed
        );
        let empty_nodes = graph.validate_bulk_nodes(operation(9_106), &[]).unwrap();
        let collision = graph
            .validate_bulk_edges(operation(9_107), &[original_edge], &empty_nodes)
            .unwrap_err();
        assert_eq!(collision.reason, BulkValidationReason::IdentityConflict);
        assert_eq!(collision.row_ordinal, Some(0));

        drop(graph);
        let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), 2);
        assert_eq!(reopened.relationship_types().unwrap(), ["KNOWS"]);
    }

    #[test]
    fn bulk_edge_failpoint_helper() {
        if std::env::var("GF_BULK_EDGE_FAILPOINT_HELPER").as_deref() != Ok("1") {
            return;
        }
        let root = std::env::var("GF_BULK_EDGE_ROOT").unwrap();
        let expect_committed = std::env::var("GF_BULK_EDGE_EXPECT_COMMITTED").unwrap() == "1";
        let graph = GraphForge::new(Some(&root)).unwrap();
        let parent = *graph.current_generation_uuid.lock().unwrap();
        let prior_catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
        let batch = edge_batch(&[uuid(5_002)], &["KNOWS"], &[uuid(5_000)], &[uuid(5_001)]);
        graph
            .publish_bulk_edges(operation(5_003), &[batch])
            .unwrap_err();
        let durable = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )
        .unwrap()
        .generation_uuid();
        let visible = *graph.current_generation_uuid.lock().unwrap();
        if expect_committed {
            assert_ne!(durable, parent);
            assert_eq!(visible, durable);
            assert_eq!(
                indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
                1
            );
            let reopened = GraphForge::new(Some(&root)).unwrap();
            assert_eq!(
                indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Edge),
                1
            );
            assert_eq!(
                graph.runtime_catalog.lock().unwrap().to_record_batch(),
                reopened.runtime_catalog.lock().unwrap().to_record_batch()
            );
        } else {
            assert_eq!(durable, parent);
            assert_eq!(visible, parent);
            assert_eq!(
                indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
                0
            );
            assert_eq!(
                graph.runtime_catalog.lock().unwrap().to_record_batch(),
                prior_catalog
            );
        }
    }

    #[test]
    fn bulk_edge_failpoints_reconcile_before_and_after_current() {
        for (failpoint, committed) in [
            ("project.before_current_replace.error", false),
            ("project.after_current_replace.error", true),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let root = dir.path().join("project");
            std::fs::create_dir(&root).unwrap();
            let graph = GraphForge::new(root.to_str()).unwrap();
            graph
                .publish_bulk_nodes(
                    operation(5_004),
                    &[node_batch(
                        &[uuid(5_000), uuid(5_001)],
                        &["Person", "Person"],
                        &[None, None],
                    )],
                )
                .unwrap();
            drop(graph);

            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("bulk_construction::tests::bulk_edge_failpoint_helper")
                .arg("--nocapture")
                .env("GF_BULK_EDGE_FAILPOINT_HELPER", "1")
                .env("GF_BULK_EDGE_ROOT", &root)
                .env(
                    "GF_BULK_EDGE_EXPECT_COMMITTED",
                    if committed { "1" } else { "0" },
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", FAILPOINT_COOKIE)
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert!(status.success(), "failpoint helper failed for {failpoint}");
        }
    }
}
