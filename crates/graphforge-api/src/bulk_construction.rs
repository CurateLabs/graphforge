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

mod normalization;
mod publication;
pub use normalization::bulk_edge_input_schema;
pub use normalization::bulk_node_input_schema;
use normalization::contract_metadata;
use normalization::uuid_column;
pub use publication::bulk_receipt_schema;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeListArray, LargeStringArray,
    ListArray, StringArray, StructArray, Time64NanosecondArray, TimestampMicrosecondArray,
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

fn open_membership_index(
    graph: &GraphForge,
    input_kind: BulkInputKind,
) -> Result<
    std::sync::MutexGuard<'_, Option<graphforge_storage::UuidMembershipIndex>>,
    BulkValidationError,
> {
    let current_generation =
        graphforge_storage::read_topology_generation(&graph.dir()).map_err(|error| {
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
    if !graphforge_storage::uuid_membership_index_present(&graph.dir()) {
        let has_nodes =
            graphforge_storage::node_topology_present(&graph.dir()).map_err(|error| {
                contract_error(
                    input_kind,
                    BulkValidationReason::ProjectState,
                    &error.to_string(),
                )
            })?;
        let has_edges = std::fs::read_dir(graph.dir().join("topology/edges"))
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
            graphforge_storage::UuidMembershipIndex::open(&graph.dir()).map_err(|error| {
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
    endpoint_candidates: &[Uuid],
    edge_candidates: Option<&[Uuid]>,
) -> Result<(HashSet<Uuid>, HashSet<Uuid>), BulkValidationError> {
    let mut index = open_membership_index(graph, BulkInputKind::Edge)?;
    let known_nodes = indexed_existing(
        index.as_mut(),
        endpoint_candidates,
        graphforge_storage::UuidIndexKind::Node,
        BulkInputKind::Edge,
    )?;
    let Some(edge_candidates) = edge_candidates else {
        return Ok((known_nodes, HashSet::new()));
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

/// Probe `candidates` (sorted, deduplicated) and return the subset the index
/// already holds. Membership only: callers never iterate the result.
fn indexed_existing(
    index: Option<&mut graphforge_storage::UuidMembershipIndex>,
    candidates: &[Uuid],
    index_kind: graphforge_storage::UuidIndexKind,
    input_kind: BulkInputKind,
) -> Result<HashSet<Uuid>, BulkValidationError> {
    let Some(index) = index else {
        return Ok(HashSet::new());
    };
    let (found, _) = index.probe(index_kind, candidates).map_err(|error| {
        contract_error(
            input_kind,
            BulkValidationReason::ProjectState,
            &error.to_string(),
        )
    })?;
    Ok(candidates
        .iter()
        .copied()
        .zip(found)
        .filter_map(|(uuid, present)| present.then_some(uuid))
        .collect())
}

/// Non-null UUIDs of `field`, sorted and deduplicated: the same set, in the
/// same order, the `BTreeSet` this replaced iterated, without one tree insert
/// per row (measured at 6.7% of validate's CPU at S18 for the three edge
/// candidate sets).
fn candidate_uuids(
    batches: &[RecordBatch],
    kind: BulkInputKind,
    field: &str,
) -> Result<Vec<Uuid>, BulkValidationError> {
    let mut values = Vec::with_capacity(batches.iter().map(RecordBatch::num_rows).sum());
    for batch in batches {
        let uuids = uuid_column(batch, kind, field)?;
        for row in 0..uuids.len() {
            if !uuids.is_null(row) {
                values.push(Uuid::from_slice(uuids.value(row)).map_err(|error| {
                    contract_error(
                        kind,
                        BulkValidationReason::SchemaMismatch,
                        &error.to_string(),
                    )
                })?);
            }
        }
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn candidate_endpoint_uuids(batches: &[RecordBatch]) -> Result<Vec<Uuid>, BulkValidationError> {
    let mut values = candidate_uuids(batches, BulkInputKind::Edge, "source_uuid")?;
    values.extend(candidate_uuids(
        batches,
        BulkInputKind::Edge,
        "target_uuid",
    )?);
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

#[cfg(test)]
fn indexed_uuid_count(graph: &GraphForge, kind: graphforge_storage::UuidIndexKind) -> u64 {
    graphforge_storage::UuidMembershipIndex::open(&graph.dir())
        .expect("published graph has an authenticated UUID membership index")
        .count(kind)
}

pub(crate) fn register_existing_endpoints(
    writer: &mut graphforge_storage::GraphWriter,
    dir: &std::path::Path,
    endpoints: &BTreeSet<Uuid>,
) -> Result<(), super::GfError> {
    if !graphforge_storage::uuid_membership_index_present(dir) {
        graphforge_storage::rebuild_uuid_membership_indexes(
            dir,
            graphforge_storage::UuidIndexBuildLimits::default(),
        )?;
    }
    if !graphforge_storage::uuid_membership_index_is_fresh(dir)? {
        return Err(super::GfError::Storage(
            "bulk endpoint UUID index is stale".into(),
        ));
    }
    let requested = endpoints.iter().copied().collect::<Vec<_>>();
    writer
        .register_existing_endpoints(&requested)
        .map_err(|error| match error {
            super::GfError::Storage(message)
                if message.contains("is absent from the authenticated node index") =>
            {
                super::GfError::Validation(
                    "bulk edge endpoint disappeared before publication".into(),
                )
            }
            other => other,
        })?;
    Ok(())
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
    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use std::process::Command;

    const FAILPOINT_COOKIE: &str = "graphforge-internal-subprocess-v1";

    pub(super) fn uuid(seed: u128) -> Uuid {
        let mut bytes = seed.to_be_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    pub(super) fn operation(seed: u128) -> OperationId {
        OperationId(uuid(seed))
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
            &graph.dir(),
            graph.ontology_mode,
            (graph.clock.lock().unwrap())().unwrap(),
        )
        .unwrap();
        let missing = uuid(70_001);
        let failure =
            register_existing_endpoints(&mut writer, &graph.dir(), &BTreeSet::from([missing]))
                .unwrap_err();
        assert_eq!(
            failure.to_string(),
            "validation error: bulk edge endpoint disappeared before publication"
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

    pub(super) fn node_batch(ids: &[Uuid], labels: &[&str], names: &[Option<&str>]) -> RecordBatch {
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

    pub(super) fn edge_batch(
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
    fn validation_is_zero_write_for_catalog_generation_and_graph_bytes() {
        let graph = GraphForge::new(None).unwrap();
        let before = crate::graph_snapshot::capture(&graph.dir()).unwrap();
        let catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
        let generation = *graph.current_generation_uuid.lock().unwrap();
        let invalid = node_batch(&[uuid(30), uuid(30)], &["Person", "Person"], &[None, None]);
        assert!(
            graph
                .validate_bulk_nodes(operation(908), &[invalid])
                .is_err()
        );
        assert_eq!(
            crate::graph_snapshot::capture(&graph.dir()).unwrap().bytes,
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
            crate::graph_snapshot::capture(&graph.dir()).unwrap().bytes,
            before.bytes
        );
        assert_eq!(
            graph.runtime_catalog.lock().unwrap().to_record_batch(),
            catalog
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
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
