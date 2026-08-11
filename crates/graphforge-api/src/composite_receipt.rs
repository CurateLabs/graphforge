//! Canonical Arrow receipt and pre-staging authorization for composite transactions.
//!
//! [`CompositeTransactionRequest::authorize_pre_staging`] composes the frozen
//! vocabulary, identity, reference, and retry decisions into one receipt. Success
//! authorizes a later staging/publication slice; this module performs no storage
//! mutation and never exposes failpoint cookies.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::canonical::uuid_v8;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::composite_transaction::{
    COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS, COMPOSITE_TRANSACTION_CONTRACT_VERSION,
    CompositeTransactionRequest,
};
use crate::composite_validation::CompositeValidationSnapshot;

/// Canonical composite receipt schema (exactly one row on success).
///
/// Field order, nullability, Arrow types, and metadata are part of the frozen
/// contract. Kind-inapplicable participant counts are zero, never null.
#[must_use]
pub fn composite_receipt_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("request_identity", DataType::FixedSizeBinary(16), false),
        Field::new("transaction_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("generation_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("content_fingerprint", DataType::FixedSizeBinary(32), false),
        Field::new("contract_version", DataType::UInt32, false),
        Field::new("graph_mutation_count", DataType::UInt64, false),
    ];
    for kind in COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS {
        fields.push(Field::new(format!("{kind}_count"), DataType::UInt64, false));
    }
    Arc::new(Schema::new_with_metadata(fields, receipt_metadata()))
}

fn receipt_metadata() -> HashMap<String, String> {
    HashMap::from([
        (
            "graphforge.composite_contract_version".to_owned(),
            COMPOSITE_TRANSACTION_CONTRACT_VERSION.to_string(),
        ),
        ("graphforge.composite_kind".to_owned(), "receipt".to_owned()),
        ("graphforge.row_order".to_owned(), "singleton".to_owned()),
        (
            "graphforge.authorization".to_owned(),
            "pre_staging".to_owned(),
        ),
    ])
}

/// Deterministic generation identity authorized by a validated composite request.
///
/// Derived only from the caller request identity and canonical content fingerprint so
/// later staging can reuse the same UUID without re-reading storage in this slice.
#[must_use]
pub fn composite_generation_uuid(request_identity: Uuid, content_fingerprint: [u8; 32]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-composite-generation/1");
    hasher.update(request_identity.as_bytes());
    hasher.update(content_fingerprint);
    uuid_v8(hasher.finalize().into())
}

/// Validate one composite request and return the canonical pre-staging Arrow receipt.
///
/// Composes request-shape, ontology, identity, graph/participant reference, and
/// idempotency decisions. `prior` is the durable receipt previously associated with
/// this request identity, when one exists. Identical content returns that receipt
/// unchanged; conflicting reuse fails with `GF_IDEMPOTENCY_CONFLICT`.
///
/// Success authorizes a later staging/publication slice. This entry point performs
/// no participant staging, journal writes, or CURRENT publication.
///
/// # Errors
/// Returns the stable structured validation, ontology, identity, not-found, or
/// idempotency conflict produced by the earliest pre-staging phase.
pub fn authorize_composite_transaction(
    request: &CompositeTransactionRequest,
    snapshot: &CompositeValidationSnapshot,
    prior: Option<([u8; 32], &RecordBatch)>,
) -> Result<RecordBatch, GfError> {
    request.authorize_pre_staging(snapshot, prior)
}

impl CompositeTransactionRequest {
    /// Validate the full pre-staging contract and return the canonical Arrow receipt.
    pub(crate) fn authorize_pre_staging(
        &self,
        snapshot: &CompositeValidationSnapshot,
        prior: Option<([u8; 32], &RecordBatch)>,
    ) -> Result<RecordBatch, GfError> {
        if let Some(prior_receipt) = self.retry_decision(prior)? {
            validate_receipt_schema(&prior_receipt)?;
            return Ok(prior_receipt);
        }
        let identities = self.validate_ontology_and_identities(snapshot)?;
        self.validate_graph_and_participant_references(snapshot, &identities)?;
        build_composite_receipt(self)
    }
}

pub(crate) fn build_composite_receipt(
    request: &CompositeTransactionRequest,
) -> Result<RecordBatch, GfError> {
    let fingerprint = request.canonical_fingerprint()?;
    let request_identity = request.request_identity().0;
    let generation_uuid = composite_generation_uuid(request_identity, fingerprint);
    let counts = request.knowledge.counts();

    let request_identities = fixed_uuid_column(&[request_identity])?;
    let transaction_uuids = fixed_uuid_column(&[request_identity])?;
    let generation_uuids = fixed_uuid_column(&[generation_uuid])?;
    let fingerprints = FixedSizeBinaryArray::try_from_iter(std::iter::once(fingerprint.as_slice()))
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let contract_versions = UInt32Array::from(vec![request.contract_version]);
    let graph_mutation_counts =
        UInt64Array::from(vec![u64::try_from(request.graph_mutations.len()).map_err(
            |_| GfError::Validation("composite graph mutation count exceeds u64".into()),
        )?]);

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(request_identities),
        Arc::new(transaction_uuids),
        Arc::new(generation_uuids),
        Arc::new(fingerprints),
        Arc::new(contract_versions),
        Arc::new(graph_mutation_counts),
    ];
    for count in counts {
        columns.push(Arc::new(UInt64Array::from(vec![
            u64::try_from(count).map_err(|_| {
                GfError::Validation("composite participant count exceeds u64".into())
            })?,
        ])));
    }

    RecordBatch::try_new(composite_receipt_schema(), columns)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn fixed_uuid_column(values: &[Uuid]) -> Result<FixedSizeBinaryArray, GfError> {
    FixedSizeBinaryArray::try_from_iter(values.iter().map(|uuid| uuid.as_bytes().as_slice()))
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn validate_receipt_schema(batch: &RecordBatch) -> Result<(), GfError> {
    let expected = composite_receipt_schema();
    if batch.schema().as_ref() != expected.as_ref() {
        return Err(GfError::Validation(
            "composite prior receipt schema does not match the frozen contract".into(),
        ));
    }
    if batch.num_rows() != 1 {
        return Err(GfError::Validation(
            "composite prior receipt must contain exactly one row".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite_transaction::{
        CompositeGraphMutation, CompositeKnowledgeParticipants, tests as transaction_tests,
    };
    use crate::composite_validation::CompositeValidationSnapshot;
    use crate::{OperationId, WriteContext};
    use arrow::array::Array;
    use graphforge_knowledge::GraphObjectKind;
    use graphforge_provenance::SubjectKind;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[..6].copy_from_slice(&[1, 2, 3, 4, 5, seed]);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    /// Aligned request with non-empty knowledge participant families and empty epistemic sets.
    fn aligned_request() -> CompositeTransactionRequest {
        let mut subject = CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(130)),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::CreateNode {
                node_uuid: uuid7(230),
                label: "Person".into(),
                properties: HashMap::new(),
            }],
            knowledge: transaction_tests::full_knowledge_fixture(),
        };
        let operation = subject.context.operation_uuid.0;
        for row in &mut subject.knowledge.provenance_events {
            *row = graphforge_provenance::ProvenanceEvent::new(
                operation,
                row.event_kind,
                None,
                row.recorded_at_micros,
            )
            .unwrap();
        }
        subject.knowledge.provenance_events.truncate(1);
        let provenance = subject.knowledge.provenance_events[0].provenance_uuid;
        let assertion = subject.knowledge.assertions[0].assertion_uuid;
        let confidence = subject.knowledge.confidence_assessments[0].confidence_uuid;
        let later_reasoning = subject.knowledge.reasoning[1].reasoning_uuid;
        subject.knowledge.lineage.truncate(1);
        let lineage = &subject.knowledge.lineage[0];
        subject.knowledge.lineage[0] = graphforge_provenance::LineageRecord::new(
            provenance,
            assertion,
            SubjectKind::Assertion,
            lineage.role,
            lineage.ordinal,
        )
        .unwrap();
        for row in &mut subject.knowledge.assertions {
            *row = graphforge_knowledge::Assertion::new(
                row.assertion_uuid,
                row.claim.clone(),
                provenance,
                row.recorded_at_micros,
            )
            .unwrap();
        }
        for (graph_ref, owner) in subject
            .knowledge
            .assertion_graph_refs
            .iter_mut()
            .zip(&subject.knowledge.assertions)
        {
            *graph_ref = graphforge_knowledge::AssertionGraphRef::new(
                owner.assertion_uuid,
                uuid7(230),
                GraphObjectKind::Node,
                graph_ref.role,
                graph_ref.ordinal,
            )
            .unwrap();
        }
        for row in &mut subject.knowledge.confidence_assessments {
            *row = graphforge_knowledge::ConfidenceAssessment::new(
                row.confidence_uuid,
                assertion,
                row.policy,
                row.value,
                provenance,
                row.recorded_at_micros,
            )
            .unwrap();
        }
        subject.knowledge.confidence_inputs.truncate(1);
        let input = &subject.knowledge.confidence_inputs[0];
        subject.knowledge.confidence_inputs[0] = graphforge_knowledge::ConfidenceInput::new(
            confidence,
            subject.knowledge.confidence_assessments[1].confidence_uuid,
            input.input_value,
            input.ordinal,
        )
        .unwrap();
        subject.knowledge.evidence.truncate(1);
        let evidence = &subject.knowledge.evidence[0];
        subject.knowledge.evidence[0] = graphforge_knowledge::EvidenceLink::new(
            evidence.evidence_uuid,
            assertion,
            evidence.source_uuid,
            graphforge_knowledge::EvidenceSourceKind::Observation,
            evidence.role,
            evidence.weight,
            provenance,
            evidence.recorded_at_micros,
        )
        .unwrap();
        for (index, row) in subject.knowledge.reasoning.iter_mut().enumerate() {
            *row = graphforge_knowledge::ReasoningRecord::new(
                row.reasoning_uuid,
                assertion,
                row.kind,
                row.content_format,
                row.content.clone(),
                (index == 0).then_some(later_reasoning),
                provenance,
                row.recorded_at_micros,
            )
            .unwrap();
        }
        subject.knowledge.assertion_status.clear();
        subject.knowledge.assertion_supersessions.clear();
        subject.knowledge.hypothesis_groups.clear();
        subject.knowledge.hypothesis_membership.clear();
        subject.knowledge.hypothesis_selection.clear();
        subject.knowledge.assertion_validity.clear();
        subject
    }

    fn empty_optional_request() -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(40)),
                actor_uuid: None,
            },
            graph_mutations: vec![
                CompositeGraphMutation::CreateNode {
                    node_uuid: uuid7(42),
                    label: "Person".into(),
                    properties: HashMap::new(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: uuid7(43),
                    rel_type: "KNOWS".into(),
                    source_uuid: uuid7(42),
                    target_uuid: uuid7(42),
                    properties: HashMap::new(),
                },
            ],
            knowledge: CompositeKnowledgeParticipants::default(),
        }
    }

    struct MutationProbe {
        staging_calls: AtomicUsize,
    }

    impl MutationProbe {
        fn new() -> Self {
            Self {
                staging_calls: AtomicUsize::new(0),
            }
        }

        fn run(
            &self,
            request: &CompositeTransactionRequest,
            snapshot: &CompositeValidationSnapshot,
            prior: Option<([u8; 32], &RecordBatch)>,
        ) -> Result<RecordBatch, GfError> {
            let first_submit = prior.is_none();
            let receipt = request.authorize_pre_staging(snapshot, prior)?;
            // Only first-submit authorization would proceed to staging in a later slice.
            if first_submit {
                self.staging_calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(receipt)
        }
    }

    #[test]
    fn receipt_schema_metadata_and_field_order_are_frozen() {
        let schema = composite_receipt_schema();
        assert_eq!(
            schema
                .metadata()
                .get("graphforge.composite_contract_version"),
            Some(&"1".to_owned())
        );
        assert_eq!(
            schema.metadata().get("graphforge.composite_kind"),
            Some(&"receipt".to_owned())
        );
        assert_eq!(
            schema.metadata().get("graphforge.row_order"),
            Some(&"singleton".to_owned())
        );
        assert_eq!(
            schema.metadata().get("graphforge.authorization"),
            Some(&"pre_staging".to_owned())
        );
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names[..6],
            [
                "request_identity",
                "transaction_uuid",
                "generation_uuid",
                "content_fingerprint",
                "contract_version",
                "graph_mutation_count",
            ]
        );
        for (index, kind) in COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS.iter().enumerate() {
            assert_eq!(names[6 + index], format!("{kind}_count"));
            assert_eq!(schema.field(6 + index).data_type(), &DataType::UInt64);
            assert!(!schema.field(6 + index).is_nullable());
        }
        assert_eq!(schema.fields().len(), 20);
    }

    #[test]
    fn valid_request_produces_canonical_singleton_receipt() {
        let request = aligned_request();
        let fingerprint = request.canonical_fingerprint().unwrap();
        let receipt = request
            .authorize_pre_staging(&CompositeValidationSnapshot::default(), None)
            .unwrap();
        assert_eq!(
            receipt.schema().as_ref(),
            composite_receipt_schema().as_ref()
        );
        assert_eq!(receipt.num_rows(), 1);

        let request_identity = receipt
            .column_by_name("request_identity")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let transaction = receipt
            .column_by_name("transaction_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let generation = receipt
            .column_by_name("generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let content = receipt
            .column_by_name("content_fingerprint")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(request_identity.value(0), uuid7(130).as_bytes());
        assert_eq!(transaction.value(0), uuid7(130).as_bytes());
        assert_eq!(content.value(0), fingerprint.as_slice());
        assert_eq!(
            generation.value(0),
            composite_generation_uuid(uuid7(130), fingerprint).as_bytes()
        );

        let graph_count = receipt
            .column_by_name("graph_mutation_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(graph_count, 1);
        let counts = request.knowledge.counts();
        for (kind, expected) in COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS
            .iter()
            .zip(counts.iter())
        {
            let actual = receipt
                .column_by_name(&format!("{kind}_count"))
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);
            assert_eq!(actual, u64::try_from(*expected).unwrap(), "{kind}");
        }
        assert!(counts[0] > 0, "provenance_events");
        assert!(counts[2] > 0, "assertions");
        assert_eq!(counts[8], 0, "assertion_status cleared in aligned fixture");
    }

    #[test]
    fn empty_optional_participant_sets_are_zero_not_null() {
        let request = empty_optional_request();
        let receipt = request
            .authorize_pre_staging(&CompositeValidationSnapshot::default(), None)
            .unwrap();
        assert_eq!(
            receipt
                .column_by_name("graph_mutation_count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
        for kind in COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS {
            let column = receipt
                .column_by_name(&format!("{kind}_count"))
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert!(!column.is_null(0));
            assert_eq!(column.value(0), 0, "{kind}");
        }
    }

    #[test]
    fn invalid_request_produces_no_receipt_and_zero_mutation() {
        let probe = MutationProbe::new();
        let mut request = aligned_request();
        request.knowledge.assertion_graph_refs[0].graph_kind = GraphObjectKind::Edge;
        let error = probe
            .run(&request, &CompositeValidationSnapshot::default(), None)
            .unwrap_err();
        assert_eq!(error.code(), "GF_NOT_FOUND");
        assert!(
            error
                .to_string()
                .contains("composite graph reference does not resolve to its declared kind")
        );
        assert_eq!(probe.staging_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn exact_retry_returns_identical_prior_receipt_without_restaging() {
        let probe = MutationProbe::new();
        let request = aligned_request();
        let fingerprint = request.canonical_fingerprint().unwrap();
        let first = probe
            .run(&request, &CompositeValidationSnapshot::default(), None)
            .unwrap();
        assert_eq!(probe.staging_calls.load(Ordering::SeqCst), 1);

        let replay = probe
            .run(
                &request,
                &CompositeValidationSnapshot::default(),
                Some((fingerprint, &first)),
            )
            .unwrap();
        assert_eq!(probe.staging_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.schema(), replay.schema());
        assert_eq!(first.num_rows(), replay.num_rows());
        for index in 0..first.num_columns() {
            assert_eq!(first.column(index).as_ref(), replay.column(index).as_ref());
        }
    }

    #[test]
    fn conflicting_identity_reuse_fails_before_staging() {
        let probe = MutationProbe::new();
        let original = aligned_request();
        let fingerprint = original.canonical_fingerprint().unwrap();
        let prior = original
            .authorize_pre_staging(&CompositeValidationSnapshot::default(), None)
            .unwrap();
        let mut conflict = original;
        if let CompositeGraphMutation::CreateNode { label, .. } = &mut conflict.graph_mutations[0] {
            *label = "Researcher".into();
        }
        let error = probe
            .run(
                &conflict,
                &CompositeValidationSnapshot::default(),
                Some((fingerprint, &prior)),
            )
            .unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(probe.staging_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn generation_uuid_is_restart_stable_for_identical_requests() {
        let first = aligned_request();
        let second = aligned_request();
        let left = first
            .authorize_pre_staging(&CompositeValidationSnapshot::default(), None)
            .unwrap();
        let right = second
            .authorize_pre_staging(&CompositeValidationSnapshot::default(), None)
            .unwrap();
        let left_generation = left
            .column_by_name("generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        let right_generation = right
            .column_by_name("generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(left_generation, right_generation);
    }
}
