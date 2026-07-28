use crate::WriteContext;
use gf_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use gf_core::{GfError, ProjectErrorCode, PropValue};
use gf_knowledge::{
    Assertion, AssertionGraphRef, AssertionGraphRole, AssertionLedger, AssertionStatusEvent,
    AssertionStatusLedger, AssertionSupersession, AssertionSupersessionLedger,
    AssertionValidityEvent, AssertionValidityLedger, ConfidenceAssessment, ConfidenceInput,
    ConfidenceLedger, EvidenceLedger, EvidenceLink, GraphObjectKind, HypothesisGroup,
    HypothesisLedger, HypothesisMembershipEvent, HypothesisSelectionEvent, MAX_KNOWLEDGE_ROWS,
    ReasoningRecord,
};
use gf_provenance::{LineageRecord, MAX_PROVENANCE_ROWS, ProvenanceEvent, ProvenanceLedger};
use std::collections::{HashMap, HashSet};
use uuid::{Uuid, Version};

/// Version of the composite request vocabulary and counting contract.
pub const COMPOSITE_TRANSACTION_CONTRACT_VERSION: u32 = 1;
/// Maximum graph mutations plus explicit participant rows in one request.
pub const MAX_COMPOSITE_TRANSACTION_ENTRIES: usize = 100_000;
/// Ordered inventory of every explicit M20/M21 participant vector.
#[rustfmt::skip]
pub const COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS: [&str; 14] = [
    "provenance_events", "lineage", "assertions", "assertion_graph_refs",
    "confidence_assessments", "confidence_inputs", "evidence", "reasoning",
    "assertion_status", "assertion_supersessions", "hypothesis_groups",
    "hypothesis_membership", "hypothesis_selection", "assertion_validity",
];

/// One explicit graph mutation. `Set*Property` covers both add and change.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompositeGraphMutation {
    /// Create a node with caller-owned identity, label, and initial properties.
    CreateNode {
        /// Stable caller-supplied UUIDv7.
        node_uuid: Uuid,
        /// Initial node label.
        label: String,
        /// Initial property values.
        properties: HashMap<String, PropValue>,
    },
    /// Create an edge with caller-owned identity and explicit endpoints.
    CreateEdge {
        /// Stable caller-supplied UUIDv7.
        edge_uuid: Uuid,
        /// Relationship type.
        rel_type: String,
        /// Source node UUID.
        source_uuid: Uuid,
        /// Target node UUID.
        target_uuid: Uuid,
        /// Initial property values.
        properties: HashMap<String, PropValue>,
    },
    /// Delete a node identified by UUID.
    DeleteNode {
        /// Node UUID.
        node_uuid: Uuid,
    },
    /// Delete an edge identified by UUID.
    DeleteEdge {
        /// Edge UUID.
        edge_uuid: Uuid,
    },
    /// Add or change one node property.
    SetNodeProperty {
        /// Node UUID.
        node_uuid: Uuid,
        /// Property name.
        property: String,
        /// New property value.
        value: PropValue,
    },
    /// Remove one node property.
    RemoveNodeProperty {
        /// Node UUID.
        node_uuid: Uuid,
        /// Property name.
        property: String,
    },
    /// Add or change one edge property.
    SetEdgeProperty {
        /// Edge UUID.
        edge_uuid: Uuid,
        /// Property name.
        property: String,
        /// New property value.
        value: PropValue,
    },
    /// Remove one edge property.
    RemoveEdgeProperty {
        /// Edge UUID.
        edge_uuid: Uuid,
        /// Property name.
        property: String,
    },
}

/// Caller-supplied domain rows. No participant is inferred from another.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompositeKnowledgeParticipants {
    /// Provenance events.
    pub provenance_events: Vec<ProvenanceEvent>,
    /// Provenance lineage records.
    pub lineage: Vec<LineageRecord>,
    /// Immutable assertions.
    pub assertions: Vec<Assertion>,
    /// Assertion-to-graph references.
    pub assertion_graph_refs: Vec<AssertionGraphRef>,
    /// Confidence assessments.
    pub confidence_assessments: Vec<ConfidenceAssessment>,
    /// Confidence input snapshots.
    pub confidence_inputs: Vec<ConfidenceInput>,
    /// Evidence links.
    pub evidence: Vec<EvidenceLink>,
    /// Reasoning records.
    pub reasoning: Vec<ReasoningRecord>,
    /// Assertion status events.
    pub assertion_status: Vec<AssertionStatusEvent>,
    /// Assertion supersession records.
    pub assertion_supersessions: Vec<AssertionSupersession>,
    /// Hypothesis groups.
    pub hypothesis_groups: Vec<HypothesisGroup>,
    /// Hypothesis membership events.
    pub hypothesis_membership: Vec<HypothesisMembershipEvent>,
    /// Hypothesis selection events.
    pub hypothesis_selection: Vec<HypothesisSelectionEvent>,
    /// Assertion validity events.
    pub assertion_validity: Vec<AssertionValidityEvent>,
}

impl CompositeKnowledgeParticipants {
    #[rustfmt::skip]
    pub(crate) fn counts(&self) -> [usize; 14] {
        [
            self.provenance_events.len(), self.lineage.len(), self.assertions.len(),
            self.assertion_graph_refs.len(), self.confidence_assessments.len(),
            self.confidence_inputs.len(), self.evidence.len(), self.reasoning.len(),
            self.assertion_status.len(), self.assertion_supersessions.len(),
            self.hypothesis_groups.len(), self.hypothesis_membership.len(),
            self.hypothesis_selection.len(), self.assertion_validity.len(),
        ]
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Frozen input for one future composite publication.
pub struct CompositeTransactionRequest {
    /// Request vocabulary contract version.
    pub contract_version: u32,
    /// Caller operation and optional actor identity.
    pub context: WriteContext,
    /// Explicit graph mutations in caller order.
    pub graph_mutations: Vec<CompositeGraphMutation>,
    /// Explicit knowledge participants; none are inferred.
    pub knowledge: CompositeKnowledgeParticipants,
}

/// Compute the canonical fingerprint of ordered graph-mutation content.
///
/// Mutation order is caller-significant. Property maps are sorted by exact UTF-8
/// key bytes, and every [`PropValue`] carries an explicit type tag. The encoding
/// is owned by Rust and does not depend on binding representation or hash-map
/// iteration order.
pub(crate) fn canonical_graph_mutation_content_fingerprint(
    mutations: &[CompositeGraphMutation],
) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFCG").map_err(canonical_error)?;
    writer
        .u32(COMPOSITE_TRANSACTION_CONTRACT_VERSION)
        .map_err(canonical_error)?;
    writer
        .u64(mutations.len() as u64)
        .map_err(canonical_error)?;
    for mutation in mutations {
        encode_graph_mutation(&mut writer, mutation)?;
    }
    fingerprint(
        CanonicalDomain::CompositeGraphMutationContent,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

impl CompositeTransactionRequest {
    /// Caller-owned request/idempotency identity, distinct from a later storage transaction UUID.
    #[must_use]
    pub(crate) const fn request_identity(&self) -> crate::OperationId {
        self.context.operation_uuid
    }

    /// Validate the request envelope and frozen domain contracts before identity lookup.
    pub(crate) fn validate_request_shape(&self) -> Result<(), GfError> {
        if self.contract_version != COMPOSITE_TRANSACTION_CONTRACT_VERSION {
            return Err(validation(
                "composite request has an unsupported contract version",
            ));
        }
        require_composite_uuid_v7(self.context.operation_uuid.0, "request identity")?;
        if self.context.actor_uuid == Some(Uuid::nil()) {
            return Err(validation(
                "composite request has an invalid actor identity",
            ));
        }
        bounded_entry_count(self.graph_mutations.len(), self.knowledge.counts())?;
        for mutation in &self.graph_mutations {
            validate_graph_mutation_shape(mutation)?;
        }

        // Fingerprinting constructs each frozen M20/M21 ledger, thereby
        // reusing its domain validation and independent resource limits.
        self.canonical_fingerprint()?;
        Ok(())
    }

    /// Canonical Rust-owned content fingerprint, excluding the request identity itself.
    ///
    /// Graph mutation order is caller-significant. Property maps and validated domain
    /// participants use their canonical order, so binding map/vector representation and
    /// process-local iteration order cannot affect the result.
    pub(crate) fn canonical_fingerprint(&self) -> Result<[u8; 32], GfError> {
        let mut writer = CanonicalWriter::new();
        writer.raw(b"GFCT").map_err(canonical_error)?;
        writer.u32(self.contract_version).map_err(canonical_error)?;
        optional_uuid(&mut writer, self.context.actor_uuid)?;
        let graph_fingerprint =
            canonical_graph_mutation_content_fingerprint(&self.graph_mutations)?;
        writer.raw(&graph_fingerprint).map_err(canonical_error)?;
        encode_knowledge(&mut writer, &self.knowledge)?;
        fingerprint(
            CanonicalDomain::CompositeRequest,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(canonical_error)
    }

    /// Resolve one pre-staging retry decision using a prior result found by request identity.
    ///
    /// `None` means first submission. An identical fingerprint returns an exact clone of the
    /// prior result; changed content fails with `GF_IDEMPOTENCY_CONFLICT` before any staging.
    pub(crate) fn retry_decision<T: Clone>(
        &self,
        prior: Option<([u8; 32], &T)>,
    ) -> Result<Option<T>, GfError> {
        let current = self.canonical_fingerprint()?;
        match prior {
            None => Ok(None),
            Some((fingerprint, result)) if fingerprint == current => Ok(Some(result.clone())),
            Some(_) => Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "composite request identity reused with different canonical content"
                    .into(),
            }),
        }
    }
}

fn encode_graph_mutation(
    writer: &mut CanonicalWriter,
    mutation: &CompositeGraphMutation,
) -> Result<(), GfError> {
    match mutation {
        CompositeGraphMutation::CreateNode {
            node_uuid,
            label,
            properties,
        } => {
            writer.u8(0).map_err(canonical_error)?;
            writer.raw(node_uuid.as_bytes()).map_err(canonical_error)?;
            writer.text(label).map_err(canonical_error)?;
            encode_properties(writer, properties)
        }
        CompositeGraphMutation::CreateEdge {
            edge_uuid,
            rel_type,
            source_uuid,
            target_uuid,
            properties,
        } => {
            writer.u8(1).map_err(canonical_error)?;
            for uuid in [edge_uuid, source_uuid, target_uuid] {
                writer.raw(uuid.as_bytes()).map_err(canonical_error)?;
            }
            writer.text(rel_type).map_err(canonical_error)?;
            encode_properties(writer, properties)
        }
        CompositeGraphMutation::DeleteNode { node_uuid } => {
            encode_uuid_mutation(writer, 2, node_uuid)
        }
        CompositeGraphMutation::DeleteEdge { edge_uuid } => {
            encode_uuid_mutation(writer, 3, edge_uuid)
        }
        CompositeGraphMutation::SetNodeProperty {
            node_uuid,
            property,
            value,
        } => encode_property_mutation(writer, 4, node_uuid, property, Some(value)),
        CompositeGraphMutation::RemoveNodeProperty {
            node_uuid,
            property,
        } => encode_property_mutation(writer, 5, node_uuid, property, None),
        CompositeGraphMutation::SetEdgeProperty {
            edge_uuid,
            property,
            value,
        } => encode_property_mutation(writer, 6, edge_uuid, property, Some(value)),
        CompositeGraphMutation::RemoveEdgeProperty {
            edge_uuid,
            property,
        } => encode_property_mutation(writer, 7, edge_uuid, property, None),
    }
}

fn encode_uuid_mutation(writer: &mut CanonicalWriter, tag: u8, uuid: &Uuid) -> Result<(), GfError> {
    writer.u8(tag).map_err(canonical_error)?;
    writer.raw(uuid.as_bytes()).map_err(canonical_error)
}

fn encode_property_mutation(
    writer: &mut CanonicalWriter,
    tag: u8,
    uuid: &Uuid,
    property: &str,
    value: Option<&PropValue>,
) -> Result<(), GfError> {
    encode_uuid_mutation(writer, tag, uuid)?;
    writer.text(property).map_err(canonical_error)?;
    value.map_or(Ok(()), |value| encode_prop_value(writer, value))
}

fn encode_properties(
    writer: &mut CanonicalWriter,
    properties: &HashMap<String, PropValue>,
) -> Result<(), GfError> {
    let mut entries = properties.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    writer.u64(entries.len() as u64).map_err(canonical_error)?;
    for (name, value) in entries {
        writer.text(name).map_err(canonical_error)?;
        encode_prop_value(writer, value)?;
    }
    Ok(())
}

fn encode_prop_value(writer: &mut CanonicalWriter, value: &PropValue) -> Result<(), GfError> {
    match value {
        PropValue::Null => writer.u8(0).map_err(canonical_error),
        PropValue::Bool(value) => {
            writer.u8(1).map_err(canonical_error)?;
            writer.u8(u8::from(*value)).map_err(canonical_error)
        }
        PropValue::Int(value) => {
            writer.u8(2).map_err(canonical_error)?;
            writer.i64(*value).map_err(canonical_error)
        }
        PropValue::Float(value) => {
            writer.u8(3).map_err(canonical_error)?;
            let bits = if *value == 0.0 {
                0
            } else if value.is_nan() {
                0x7ff8_0000_0000_0000
            } else {
                value.to_bits()
            };
            writer.u64(bits).map_err(canonical_error)
        }
        PropValue::Str(value) => {
            writer.u8(4).map_err(canonical_error)?;
            writer.text(value).map_err(canonical_error)
        }
        PropValue::List(values) => {
            writer.u8(5).map_err(canonical_error)?;
            writer.u64(values.len() as u64).map_err(canonical_error)?;
            for value in values {
                encode_prop_value(writer, value)?;
            }
            Ok(())
        }
        _ => Err(GfError::Validation(
            "unsupported property value variant in canonical graph mutation content".into(),
        )),
    }
}

fn canonical_error(error: gf_core::canonical::CanonicalError) -> GfError {
    GfError::Validation(Box::new(error).to_string())
}

fn optional_uuid(writer: &mut CanonicalWriter, value: Option<Uuid>) -> Result<(), GfError> {
    writer
        .u8(u8::from(value.is_some()))
        .map_err(canonical_error)?;
    value.map_or(Ok(()), |uuid| {
        writer.raw(uuid.as_bytes()).map_err(canonical_error)
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "the frozen 14-family inventory is intentionally encoded in one auditable sequence"
)]
fn encode_knowledge(
    writer: &mut CanonicalWriter,
    value: &CompositeKnowledgeParticipants,
) -> Result<(), GfError> {
    let provenance_ids = value
        .provenance_events
        .iter()
        .map(|row| row.provenance_uuid)
        .collect::<HashSet<_>>();
    let (local_lineage, mut external_lineage) = composite_lineage(&value.lineage, &provenance_ids)?;
    let assertion_ids = value
        .assertions
        .iter()
        .map(|row| row.assertion_uuid)
        .collect::<HashSet<_>>();
    let (local_graph_refs, mut external_graph_refs): (Vec<_>, Vec<_>) = value
        .assertion_graph_refs
        .iter()
        .cloned()
        .partition(|row| assertion_ids.contains(&row.assertion_uuid));
    validate_external_graph_refs(&external_graph_refs)?;

    let confidence_ids = value
        .confidence_assessments
        .iter()
        .map(|row| row.confidence_uuid)
        .collect::<HashSet<_>>();
    let (local_confidence_inputs, mut external_confidence_inputs): (Vec<_>, Vec<_>) = value
        .confidence_inputs
        .iter()
        .cloned()
        .partition(|row| confidence_ids.contains(&row.confidence_uuid));
    validate_external_confidence_inputs(&external_confidence_inputs)?;

    let group_ids = value
        .hypothesis_groups
        .iter()
        .map(|row| row.group_uuid)
        .collect::<HashSet<_>>();
    let (local_membership, mut external_membership): (Vec<_>, Vec<_>) = value
        .hypothesis_membership
        .iter()
        .cloned()
        .partition(|row| group_ids.contains(&row.group_uuid));
    let (local_selection, mut external_selection): (Vec<_>, Vec<_>) = value
        .hypothesis_selection
        .iter()
        .cloned()
        .partition(|row| group_ids.contains(&row.group_uuid));
    validate_hypothesis_event_ids(&value.hypothesis_membership, &value.hypothesis_selection)?;
    validate_external_hypothesis_events(&external_membership, &external_selection)?;

    let provenance = ProvenanceLedger::new(value.provenance_events.clone(), local_lineage)
        .map_err(domain_error)?;
    let assertions =
        AssertionLedger::new(value.assertions.clone(), local_graph_refs).map_err(domain_error)?;
    let confidence = ConfidenceLedger::new(
        value.confidence_assessments.clone(),
        local_confidence_inputs,
    )
    .map_err(domain_error)?;
    let evidence = EvidenceLedger::new(value.evidence.clone()).map_err(domain_error)?;
    let reasoning = composite_reasoning(value.reasoning.clone())?;
    let status =
        AssertionStatusLedger::new(value.assertion_status.clone()).map_err(domain_error)?;
    let supersessions = AssertionSupersessionLedger::new(value.assertion_supersessions.clone())
        .map_err(domain_error)?;
    let hypotheses = HypothesisLedger::new(
        value.hypothesis_groups.clone(),
        local_membership,
        local_selection,
    )
    .map_err(domain_error)?;
    let validity =
        AssertionValidityLedger::new(value.assertion_validity.clone()).map_err(domain_error)?;
    let counts = [
        value.provenance_events.len(),
        value.lineage.len(),
        value.assertions.len(),
        value.assertion_graph_refs.len(),
        value.confidence_assessments.len(),
        value.confidence_inputs.len(),
        value.evidence.len(),
        value.reasoning.len(),
        value.assertion_status.len(),
        value.assertion_supersessions.len(),
        value.hypothesis_groups.len(),
        value.hypothesis_membership.len(),
        value.hypothesis_selection.len(),
        value.assertion_validity.len(),
    ];
    for count in counts {
        writer.u64(count as u64).map_err(canonical_error)?;
    }
    for event in &provenance.events {
        encode_owned(
            writer,
            event.provenance_uuid,
            event.contract_version,
            event.fingerprint().map_err(domain_error)?,
        )?;
    }
    for row in &provenance.lineage {
        encode_owned(
            writer,
            row.lineage_uuid,
            row.contract_version,
            row.fingerprint().map_err(domain_error)?,
        )?;
    }
    external_lineage.sort_by_key(|row| {
        (
            row.provenance_uuid,
            provenance_role_order(row.role),
            row.ordinal,
            row.subject_uuid,
        )
    });
    encode_external_count(writer, external_lineage.len())?;
    for row in &external_lineage {
        encode_owned(
            writer,
            row.lineage_uuid,
            row.contract_version,
            row.fingerprint().map_err(domain_error)?,
        )?;
    }
    for row in &assertions.assertions {
        encode_owned(
            writer,
            row.assertion_uuid,
            row.contract_version,
            assertions
                .assertion_fingerprint(row.assertion_uuid)
                .map_err(domain_error)?,
        )?;
    }
    external_graph_refs.sort_by_key(|row| {
        (
            row.assertion_uuid,
            graph_role_order(row.role),
            row.ordinal,
            graph_kind_order(row.graph_kind),
            row.graph_uuid,
        )
    });
    encode_external_count(writer, external_graph_refs.len())?;
    for row in &external_graph_refs {
        encode_owned(
            writer,
            row.assertion_uuid,
            row.contract_version,
            external_graph_ref_fingerprint(row)?,
        )?;
    }
    for row in &confidence.assessments {
        encode_owned(
            writer,
            row.confidence_uuid,
            row.contract_version,
            confidence
                .assessment_fingerprint(row.confidence_uuid)
                .map_err(domain_error)?,
        )?;
    }
    external_confidence_inputs
        .sort_by_key(|row| (row.confidence_uuid, row.ordinal, row.input_confidence_uuid));
    encode_external_count(writer, external_confidence_inputs.len())?;
    for row in &external_confidence_inputs {
        encode_owned(
            writer,
            row.confidence_uuid,
            row.contract_version,
            external_confidence_input_fingerprint(row)?,
        )?;
    }
    for row in &evidence.links {
        encode_owned(
            writer,
            row.evidence_uuid,
            row.contract_version,
            evidence
                .evidence_fingerprint(row.evidence_uuid)
                .map_err(domain_error)?,
        )?;
    }
    for row in &reasoning {
        encode_owned(
            writer,
            row.reasoning_uuid,
            row.contract_version,
            composite_reasoning_fingerprint(row)?,
        )?;
    }
    for row in &status.events {
        encode_owned(
            writer,
            row.status_event_uuid,
            row.contract_version,
            status
                .event_fingerprint(row.status_event_uuid)
                .map_err(domain_error)?,
        )?;
    }
    for row in supersessions.relations() {
        encode_owned(
            writer,
            row.supersession_uuid,
            row.contract_version,
            supersessions
                .relation_fingerprint(row.supersession_uuid)
                .map_err(domain_error)?,
        )?;
    }
    for row in hypotheses.groups() {
        encode_owned(
            writer,
            row.group_uuid,
            row.contract_version,
            hypotheses
                .group_fingerprint(row.group_uuid)
                .map_err(domain_error)?,
        )?;
    }
    for row in hypotheses.membership_events() {
        encode_owned(
            writer,
            row.membership_event_uuid,
            row.contract_version,
            hypotheses
                .membership_fingerprint(row.membership_event_uuid)
                .map_err(domain_error)?,
        )?;
    }
    external_membership.sort_by_key(|row| (row.recorded_at_micros, row.membership_event_uuid));
    encode_external_count(writer, external_membership.len())?;
    for row in &external_membership {
        encode_owned(
            writer,
            row.membership_event_uuid,
            row.contract_version,
            hypothesis_membership_fingerprint(row)?,
        )?;
    }
    for row in hypotheses.selection_events() {
        encode_owned(
            writer,
            row.selection_event_uuid,
            row.contract_version,
            hypotheses
                .selection_fingerprint(row.selection_event_uuid)
                .map_err(domain_error)?,
        )?;
    }
    external_selection.sort_by_key(|row| (row.recorded_at_micros, row.selection_event_uuid));
    encode_external_count(writer, external_selection.len())?;
    for row in &external_selection {
        encode_owned(
            writer,
            row.selection_event_uuid,
            row.contract_version,
            hypothesis_selection_fingerprint(row)?,
        )?;
    }
    for row in &validity.events {
        encode_owned(
            writer,
            row.validity_event_uuid,
            row.contract_version,
            validity
                .event_fingerprint(row.validity_event_uuid)
                .map_err(domain_error)?,
        )?;
    }
    Ok(())
}

fn composite_lineage(
    rows: &[LineageRecord],
    local_provenance: &HashSet<Uuid>,
) -> Result<(Vec<LineageRecord>, Vec<LineageRecord>), GfError> {
    if rows.len() > MAX_PROVENANCE_ROWS {
        return Err(domain_error(format!(
            "provenance lineage row limit exceeded: observed {}, limit {MAX_PROVENANCE_ROWS}",
            rows.len()
        )));
    }
    let mut identities = HashSet::with_capacity(rows.len());
    let mut positions = HashSet::with_capacity(rows.len());
    for row in rows {
        let rebuilt = LineageRecord::new(
            row.provenance_uuid,
            row.subject_uuid,
            row.subject_kind,
            row.role,
            row.ordinal,
        )
        .map_err(domain_error)?;
        if rebuilt != *row {
            return Err(domain_error("lineage row is not canonical"));
        }
        if !identities.insert(row.lineage_uuid) {
            return Err(domain_error("duplicate provenance identity: lineage_uuid"));
        }
        if !positions.insert((row.provenance_uuid, row.role, row.ordinal)) {
            return Err(domain_error("duplicate provenance identity: role/ordinal"));
        }
    }
    Ok(rows
        .iter()
        .cloned()
        .partition(|row| local_provenance.contains(&row.provenance_uuid)))
}

fn composite_reasoning(mut rows: Vec<ReasoningRecord>) -> Result<Vec<ReasoningRecord>, GfError> {
    if rows.len() > MAX_KNOWLEDGE_ROWS {
        return Err(domain_error(format!(
            "knowledge reasoning row limit exceeded: observed {}, limit {MAX_KNOWLEDGE_ROWS}",
            rows.len()
        )));
    }
    let mut by_id = HashMap::with_capacity(rows.len());
    for row in &rows {
        let rebuilt = ReasoningRecord::new(
            row.reasoning_uuid,
            row.assertion_uuid,
            row.kind,
            row.content_format,
            row.content.clone(),
            row.supersedes_reasoning_uuid,
            row.provenance_uuid,
            row.recorded_at_micros,
        )
        .map_err(domain_error)?;
        if rebuilt != *row {
            return Err(domain_error("reasoning row is not canonical"));
        }
        if by_id.insert(row.reasoning_uuid, row).is_some() {
            return Err(domain_error("duplicate knowledge identity: reasoning_uuid"));
        }
    }
    let mut verified = HashSet::with_capacity(rows.len());
    let mut visiting = HashSet::with_capacity(rows.len());
    let mut path = Vec::new();
    for row in &rows {
        if verified.contains(&row.reasoning_uuid) {
            continue;
        }
        path.clear();
        let mut current = row;
        loop {
            if verified.contains(&current.reasoning_uuid) {
                break;
            }
            if !visiting.insert(current.reasoning_uuid) {
                return Err(domain_error(
                    "invalid knowledge reasoning.supersedes_reasoning_uuid: cycle is forbidden",
                ));
            }
            path.push(current.reasoning_uuid);
            let Some(previous_uuid) = current.supersedes_reasoning_uuid else {
                break;
            };
            let Some(previous) = by_id.get(&previous_uuid) else {
                break;
            };
            if previous.assertion_uuid != row.assertion_uuid {
                return Err(domain_error(
                    "invalid knowledge reasoning.supersedes_reasoning_uuid: cross-assertion amendment is forbidden",
                ));
            }
            current = previous;
        }
        for reasoning_uuid in path.drain(..) {
            visiting.remove(&reasoning_uuid);
            verified.insert(reasoning_uuid);
        }
    }
    rows.sort_by_key(|row| (row.recorded_at_micros, row.reasoning_uuid));
    Ok(rows)
}

fn composite_reasoning_fingerprint(row: &ReasoningRecord) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    writer
        .raw(row.reasoning_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .raw(row.assertion_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer.text(row.kind.as_str()).map_err(canonical_error)?;
    writer
        .text(row.content_format.as_str())
        .map_err(canonical_error)?;
    writer.binary(&row.content).map_err(canonical_error)?;
    match row.supersedes_reasoning_uuid {
        Some(value) => {
            writer.u8(1).map_err(canonical_error)?;
            writer.raw(value.as_bytes()).map_err(canonical_error)?;
        }
        None => writer.u8(0).map_err(canonical_error)?,
    }
    writer
        .raw(row.provenance_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .i64(row.recorded_at_micros)
        .map_err(canonical_error)?;
    writer.u32(row.contract_version).map_err(canonical_error)?;
    fingerprint(
        CanonicalDomain::Reasoning,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

const fn provenance_role_order(role: gf_provenance::LineageRole) -> u8 {
    match role {
        gf_provenance::LineageRole::Input => 0,
        gf_provenance::LineageRole::Output => 1,
    }
}

fn encode_external_count(writer: &mut CanonicalWriter, count: usize) -> Result<(), GfError> {
    if count != 0 {
        writer.raw(b"GFEX").map_err(canonical_error)?;
        writer.u64(count as u64).map_err(canonical_error)?;
    }
    Ok(())
}

fn validate_external_graph_refs(rows: &[AssertionGraphRef]) -> Result<(), GfError> {
    let mut identities = HashSet::with_capacity(rows.len());
    for row in rows {
        let reconstructed = AssertionGraphRef::new(
            row.assertion_uuid,
            row.graph_uuid,
            row.graph_kind,
            row.role,
            row.ordinal,
        )
        .map_err(domain_error)?;
        if reconstructed != *row {
            return Err(domain_error(
                "assertion_graph_ref.contract_version is unsupported",
            ));
        }
        if !identities.insert((row.assertion_uuid, row.graph_uuid, row.role, row.ordinal)) {
            return Err(domain_error(
                "duplicate assertion_uuid/graph_uuid/role/ordinal",
            ));
        }
    }
    Ok(())
}

fn validate_external_confidence_inputs(rows: &[ConfidenceInput]) -> Result<(), GfError> {
    let mut identities = HashSet::with_capacity(rows.len());
    for row in rows {
        let reconstructed = ConfidenceInput::new(
            row.confidence_uuid,
            row.input_confidence_uuid,
            row.input_value,
            row.ordinal,
        )
        .map_err(domain_error)?;
        if reconstructed != *row {
            return Err(domain_error(
                "confidence_input.contract_version is unsupported",
            ));
        }
        if !identities.insert((row.confidence_uuid, row.input_confidence_uuid)) {
            return Err(domain_error("duplicate confidence input identity"));
        }
    }
    Ok(())
}

fn validate_hypothesis_event_ids(
    membership: &[HypothesisMembershipEvent],
    selection: &[HypothesisSelectionEvent],
) -> Result<(), GfError> {
    let mut identities = HashSet::with_capacity(membership.len());
    for row in membership {
        if !identities.insert(row.membership_event_uuid) {
            return Err(domain_error("duplicate membership_event_uuid"));
        }
    }
    identities.clear();
    for row in selection {
        if !identities.insert(row.selection_event_uuid) {
            return Err(domain_error("duplicate selection_event_uuid"));
        }
    }
    Ok(())
}

fn validate_external_hypothesis_events(
    membership: &[HypothesisMembershipEvent],
    selection: &[HypothesisSelectionEvent],
) -> Result<(), GfError> {
    for row in membership {
        let reconstructed = HypothesisMembershipEvent::new(
            row.membership_event_uuid,
            row.operation_uuid,
            row.group_uuid,
            row.assertion_uuid,
            row.action,
            row.reasoning_uuid,
            row.provenance_uuid,
            row.recorded_at_micros,
        )
        .map_err(domain_error)?;
        if reconstructed != *row {
            return Err(domain_error(
                "hypothesis_membership.contract_version is unsupported",
            ));
        }
    }
    for row in selection {
        let reconstructed = HypothesisSelectionEvent::new(
            row.selection_event_uuid,
            row.operation_uuid,
            row.group_uuid,
            row.selected_assertion_uuid,
            row.reasoning_uuid,
            row.provenance_uuid,
            row.recorded_at_micros,
        )
        .map_err(domain_error)?;
        if reconstructed != *row {
            return Err(domain_error(
                "hypothesis_selection.contract_version is unsupported",
            ));
        }
    }
    Ok(())
}

fn external_graph_ref_fingerprint(row: &AssertionGraphRef) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFAR").map_err(canonical_error)?;
    writer
        .raw(row.assertion_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer.text(row.role.as_str()).map_err(canonical_error)?;
    writer.u32(row.ordinal).map_err(canonical_error)?;
    writer
        .text(row.graph_kind.as_str())
        .map_err(canonical_error)?;
    writer
        .raw(row.graph_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer.u32(row.contract_version).map_err(canonical_error)?;
    fingerprint(
        CanonicalDomain::CompositeRequest,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn external_confidence_input_fingerprint(row: &ConfidenceInput) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFCI").map_err(canonical_error)?;
    writer
        .raw(row.confidence_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .raw(row.input_confidence_uuid.as_bytes())
        .map_err(canonical_error)?;
    canonical_optional_f64(&mut writer, row.input_value)?;
    writer.u32(row.ordinal).map_err(canonical_error)?;
    writer.u32(row.contract_version).map_err(canonical_error)?;
    fingerprint(
        CanonicalDomain::CompositeRequest,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn hypothesis_membership_fingerprint(row: &HypothesisMembershipEvent) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    for value in [
        row.membership_event_uuid,
        row.operation_uuid,
        row.group_uuid,
        row.assertion_uuid,
    ] {
        writer.raw(value.as_bytes()).map_err(canonical_error)?;
    }
    writer.text(row.action.as_str()).map_err(canonical_error)?;
    writer
        .raw(row.reasoning_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .raw(row.provenance_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .i64(row.recorded_at_micros)
        .map_err(canonical_error)?;
    writer.u32(row.contract_version).map_err(canonical_error)?;
    fingerprint(
        CanonicalDomain::HypothesisMembership,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn hypothesis_selection_fingerprint(row: &HypothesisSelectionEvent) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    for value in [row.selection_event_uuid, row.operation_uuid, row.group_uuid] {
        writer.raw(value.as_bytes()).map_err(canonical_error)?;
    }
    match row.selected_assertion_uuid {
        Some(value) => {
            writer.u8(1).map_err(canonical_error)?;
            writer.raw(value.as_bytes()).map_err(canonical_error)?;
        }
        None => writer.u8(0).map_err(canonical_error)?,
    }
    writer
        .raw(row.reasoning_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .raw(row.provenance_uuid.as_bytes())
        .map_err(canonical_error)?;
    writer
        .i64(row.recorded_at_micros)
        .map_err(canonical_error)?;
    writer.u32(row.contract_version).map_err(canonical_error)?;
    fingerprint(
        CanonicalDomain::HypothesisSelection,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn canonical_optional_f64(writer: &mut CanonicalWriter, value: Option<f64>) -> Result<(), GfError> {
    match value {
        None => writer.u8(0).map_err(canonical_error),
        Some(value) => {
            writer.u8(1).map_err(canonical_error)?;
            writer
                .u64(if value == 0.0 { 0.0 } else { value }.to_bits())
                .map_err(canonical_error)
        }
    }
}

const fn graph_role_order(role: AssertionGraphRole) -> u8 {
    match role {
        AssertionGraphRole::Subject => 0,
        AssertionGraphRole::Object => 1,
        AssertionGraphRole::Context => 2,
    }
}

const fn graph_kind_order(kind: GraphObjectKind) -> u8 {
    match kind {
        GraphObjectKind::Node => 0,
        GraphObjectKind::Edge => 1,
    }
}

fn encode_owned(
    writer: &mut CanonicalWriter,
    identity: Uuid,
    version: u32,
    digest: [u8; 32],
) -> Result<(), GfError> {
    writer.raw(identity.as_bytes()).map_err(canonical_error)?;
    writer.u32(version).map_err(canonical_error)?;
    writer.raw(&digest).map_err(canonical_error)
}

fn domain_error(error: impl std::fmt::Display) -> GfError {
    GfError::Validation(format!("invalid composite participant: {error}"))
}

fn validate_graph_mutation_shape(mutation: &CompositeGraphMutation) -> Result<(), GfError> {
    match mutation {
        CompositeGraphMutation::CreateNode { node_uuid, .. } => {
            require_composite_uuid_v7(*node_uuid, "node identity")?;
        }
        CompositeGraphMutation::CreateEdge {
            edge_uuid,
            source_uuid,
            target_uuid,
            ..
        } => {
            require_composite_uuid_v7(*edge_uuid, "edge identity")?;
            require_composite_uuid(*source_uuid, "edge endpoint")?;
            require_composite_uuid(*target_uuid, "edge endpoint")?;
        }
        CompositeGraphMutation::DeleteNode { node_uuid }
        | CompositeGraphMutation::SetNodeProperty { node_uuid, .. }
        | CompositeGraphMutation::RemoveNodeProperty { node_uuid, .. } => {
            require_composite_uuid(*node_uuid, "node target")?;
        }
        CompositeGraphMutation::DeleteEdge { edge_uuid }
        | CompositeGraphMutation::SetEdgeProperty { edge_uuid, .. }
        | CompositeGraphMutation::RemoveEdgeProperty { edge_uuid, .. } => {
            require_composite_uuid(*edge_uuid, "edge target")?;
        }
    }
    Ok(())
}

fn require_composite_uuid_v7(uuid: Uuid, kind: &'static str) -> Result<(), GfError> {
    if uuid.get_version() == Some(Version::SortRand) {
        Ok(())
    } else {
        Err(validation(&format!(
            "composite request has an invalid {kind}"
        )))
    }
}

fn require_composite_uuid(uuid: Uuid, kind: &'static str) -> Result<(), GfError> {
    if uuid.is_nil() {
        Err(validation(&format!(
            "composite request has an invalid {kind}"
        )))
    } else {
        Ok(())
    }
}

fn aggregate_entry_count(graph: usize, knowledge: [usize; 14]) -> Result<usize, GfError> {
    knowledge
        .into_iter()
        .try_fold(graph, usize::checked_add)
        .ok_or_else(|| validation("composite transaction entry count overflow"))
}

fn bounded_entry_count(graph: usize, knowledge: [usize; 14]) -> Result<usize, GfError> {
    let total = aggregate_entry_count(graph, knowledge)?;
    if total > MAX_COMPOSITE_TRANSACTION_ENTRIES {
        return Err(validation(&format!(
            "composite transaction entry limit exceeded: {total} > {MAX_COMPOSITE_TRANSACTION_ENTRIES}"
        )));
    }
    Ok(total)
}

fn validation(message: &str) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::OperationId;
    use gf_knowledge::{
        AssertionGraphRole, AssertionStatus, ConfidencePolicy, EvidenceRole, EvidenceSourceKind,
        GraphObjectKind, HypothesisMembershipAction, ReasoningContentFormat, ReasoningKind,
        ReasoningLedger,
    };
    use gf_provenance::{EventKind, LineageRole, SubjectKind};

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    pub(crate) fn full_knowledge_fixture() -> CompositeKnowledgeParticipants {
        let provenance_events = [
            ProvenanceEvent::new(uuid7(1), EventKind::CreateNode, None, 10).unwrap(),
            ProvenanceEvent::new(uuid7(2), EventKind::CreateNode, None, 20).unwrap(),
        ];
        let assertions = [
            Assertion::new(
                uuid7(20),
                "claim-a".into(),
                provenance_events[0].provenance_uuid,
                10,
            )
            .unwrap(),
            Assertion::new(
                uuid7(21),
                "claim-b".into(),
                provenance_events[1].provenance_uuid,
                20,
            )
            .unwrap(),
        ];
        let confidence_assessments = [
            ConfidenceAssessment::new(
                uuid7(40),
                assertions[0].assertion_uuid,
                ConfidencePolicy::ConservativeMin,
                None,
                provenance_events[0].provenance_uuid,
                10,
            )
            .unwrap(),
            ConfidenceAssessment::new(
                uuid7(41),
                assertions[1].assertion_uuid,
                ConfidencePolicy::ConservativeMin,
                None,
                provenance_events[1].provenance_uuid,
                20,
            )
            .unwrap(),
        ];
        let reasoning = [
            ReasoningRecord::new(
                uuid7(60),
                assertions[0].assertion_uuid,
                ReasoningKind::LogicalInference,
                ReasoningContentFormat::TextPlain,
                b"reason-a".to_vec(),
                None,
                provenance_events[0].provenance_uuid,
                10,
            )
            .unwrap(),
            ReasoningRecord::new(
                uuid7(61),
                assertions[1].assertion_uuid,
                ReasoningKind::LogicalInference,
                ReasoningContentFormat::TextPlain,
                b"reason-b".to_vec(),
                None,
                provenance_events[1].provenance_uuid,
                20,
            )
            .unwrap(),
        ];
        let assertion_status = [
            AssertionStatusEvent::new(
                uuid7(70),
                assertions[0].assertion_uuid,
                AssertionStatus::Supported,
                Some(confidence_assessments[0].confidence_uuid),
                Some(reasoning[0].reasoning_uuid),
                provenance_events[0].provenance_uuid,
                10,
            )
            .unwrap(),
            AssertionStatusEvent::new(
                uuid7(71),
                assertions[1].assertion_uuid,
                AssertionStatus::Supported,
                Some(confidence_assessments[1].confidence_uuid),
                Some(reasoning[1].reasoning_uuid),
                provenance_events[1].provenance_uuid,
                20,
            )
            .unwrap(),
        ];
        let hypothesis_groups = [
            HypothesisGroup::new(
                uuid7(90),
                "question.a".into(),
                provenance_events[0].provenance_uuid,
                1,
            )
            .unwrap(),
            HypothesisGroup::new(
                uuid7(91),
                "question.b".into(),
                provenance_events[1].provenance_uuid,
                2,
            )
            .unwrap(),
        ];
        CompositeKnowledgeParticipants {
            provenance_events: provenance_events.to_vec(),
            lineage: vec![
                LineageRecord::new(
                    provenance_events[0].provenance_uuid,
                    uuid7(30),
                    SubjectKind::Node,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance_events[1].provenance_uuid,
                    uuid7(31),
                    SubjectKind::Node,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
            ],
            assertions: assertions.to_vec(),
            assertion_graph_refs: vec![
                AssertionGraphRef::new(
                    assertions[0].assertion_uuid,
                    uuid7(30),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
                AssertionGraphRef::new(
                    assertions[1].assertion_uuid,
                    uuid7(31),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
            confidence_assessments: confidence_assessments.to_vec(),
            confidence_inputs: vec![
                ConfidenceInput::new(uuid7(40), uuid7(42), None, 0).unwrap(),
                ConfidenceInput::new(uuid7(41), uuid7(43), None, 0).unwrap(),
            ],
            evidence: vec![
                EvidenceLink::new(
                    uuid7(50),
                    assertions[0].assertion_uuid,
                    uuid7(52),
                    EvidenceSourceKind::Document,
                    EvidenceRole::Supports,
                    Some(0.8),
                    provenance_events[0].provenance_uuid,
                    10,
                )
                .unwrap(),
                EvidenceLink::new(
                    uuid7(51),
                    assertions[1].assertion_uuid,
                    uuid7(53),
                    EvidenceSourceKind::Observation,
                    EvidenceRole::Context,
                    Some(0.6),
                    provenance_events[1].provenance_uuid,
                    20,
                )
                .unwrap(),
            ],
            reasoning: reasoning.to_vec(),
            assertion_status: assertion_status.to_vec(),
            assertion_supersessions: vec![
                AssertionSupersession::new(
                    uuid7(80),
                    assertions[0].assertion_uuid,
                    uuid7(22),
                    assertion_status[0].status_event_uuid,
                    reasoning[0].reasoning_uuid,
                    provenance_events[0].provenance_uuid,
                    10,
                )
                .unwrap(),
                AssertionSupersession::new(
                    uuid7(81),
                    assertions[1].assertion_uuid,
                    uuid7(23),
                    assertion_status[1].status_event_uuid,
                    reasoning[1].reasoning_uuid,
                    provenance_events[1].provenance_uuid,
                    20,
                )
                .unwrap(),
            ],
            hypothesis_groups: hypothesis_groups.to_vec(),
            hypothesis_membership: vec![
                HypothesisMembershipEvent::new(
                    uuid7(100),
                    uuid7(102),
                    hypothesis_groups[0].group_uuid,
                    assertions[0].assertion_uuid,
                    HypothesisMembershipAction::Added,
                    reasoning[0].reasoning_uuid,
                    provenance_events[0].provenance_uuid,
                    10,
                )
                .unwrap(),
                HypothesisMembershipEvent::new(
                    uuid7(101),
                    uuid7(103),
                    hypothesis_groups[1].group_uuid,
                    assertions[1].assertion_uuid,
                    HypothesisMembershipAction::Added,
                    reasoning[1].reasoning_uuid,
                    provenance_events[1].provenance_uuid,
                    11,
                )
                .unwrap(),
            ],
            hypothesis_selection: vec![
                HypothesisSelectionEvent::new(
                    uuid7(110),
                    uuid7(112),
                    hypothesis_groups[0].group_uuid,
                    Some(assertions[0].assertion_uuid),
                    reasoning[0].reasoning_uuid,
                    provenance_events[0].provenance_uuid,
                    20,
                )
                .unwrap(),
                HypothesisSelectionEvent::new(
                    uuid7(111),
                    uuid7(113),
                    hypothesis_groups[1].group_uuid,
                    Some(assertions[1].assertion_uuid),
                    reasoning[1].reasoning_uuid,
                    provenance_events[1].provenance_uuid,
                    21,
                )
                .unwrap(),
            ],
            assertion_validity: vec![
                AssertionValidityEvent::new(
                    uuid7(120),
                    assertions[0].assertion_uuid,
                    Some(0),
                    Some(100),
                    Some(reasoning[0].reasoning_uuid),
                    provenance_events[0].provenance_uuid,
                    10,
                )
                .unwrap(),
                AssertionValidityEvent::new(
                    uuid7(121),
                    assertions[1].assertion_uuid,
                    Some(10),
                    None,
                    Some(reasoning[1].reasoning_uuid),
                    provenance_events[1].provenance_uuid,
                    20,
                )
                .unwrap(),
            ],
        }
    }

    fn request(operation_seed: u8, actor_seed: u8) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(operation_seed)),
                actor_uuid: Some(uuid7(actor_seed)),
            },
            graph_mutations: vec![CompositeGraphMutation::CreateNode {
                node_uuid: uuid7(30),
                label: "Person".into(),
                properties: HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            }],
            knowledge: full_knowledge_fixture(),
        }
    }

    fn existing_owner_request() -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid7(140)),
                actor_uuid: Some(uuid7(141)),
            },
            graph_mutations: Vec::new(),
            knowledge: CompositeKnowledgeParticipants {
                assertion_graph_refs: vec![
                    AssertionGraphRef::new(
                        uuid7(142),
                        uuid7(143),
                        GraphObjectKind::Node,
                        AssertionGraphRole::Subject,
                        0,
                    )
                    .unwrap(),
                    AssertionGraphRef::new(
                        uuid7(144),
                        uuid7(145),
                        GraphObjectKind::Edge,
                        AssertionGraphRole::Context,
                        2,
                    )
                    .unwrap(),
                ],
                confidence_inputs: vec![
                    ConfidenceInput::new(uuid7(146), uuid7(147), Some(0.25), 1).unwrap(),
                    ConfidenceInput::new(uuid7(148), uuid7(149), None, 0).unwrap(),
                ],
                hypothesis_membership: vec![
                    HypothesisMembershipEvent::new(
                        uuid7(150),
                        uuid7(151),
                        uuid7(152),
                        uuid7(153),
                        HypothesisMembershipAction::Added,
                        uuid7(154),
                        uuid7(155),
                        30,
                    )
                    .unwrap(),
                    HypothesisMembershipEvent::new(
                        uuid7(156),
                        uuid7(157),
                        uuid7(158),
                        uuid7(159),
                        HypothesisMembershipAction::Removed,
                        uuid7(160),
                        uuid7(161),
                        20,
                    )
                    .unwrap(),
                ],
                hypothesis_selection: vec![
                    HypothesisSelectionEvent::new(
                        uuid7(162),
                        uuid7(163),
                        uuid7(152),
                        Some(uuid7(153)),
                        uuid7(164),
                        uuid7(165),
                        40,
                    )
                    .unwrap(),
                    HypothesisSelectionEvent::new(
                        uuid7(166),
                        uuid7(167),
                        uuid7(158),
                        None,
                        uuid7(168),
                        uuid7(169),
                        10,
                    )
                    .unwrap(),
                ],
                ..CompositeKnowledgeParticipants::default()
            },
        }
    }

    fn reverse_participant_rows(value: &mut CompositeKnowledgeParticipants) {
        value.provenance_events.reverse();
        value.lineage.reverse();
        value.assertions.reverse();
        value.assertion_graph_refs.reverse();
        value.confidence_assessments.reverse();
        value.confidence_inputs.reverse();
        value.evidence.reverse();
        value.reasoning.reverse();
        value.assertion_status.reverse();
        value.assertion_supersessions.reverse();
        value.hypothesis_groups.reverse();
        value.hypothesis_membership.reverse();
        value.hypothesis_selection.reverse();
        value.assertion_validity.reverse();
    }

    #[test]
    fn all_participant_families_are_composed_in_canonical_order() {
        let full = full_knowledge_fixture();
        assert_eq!(full.counts(), [2; 14]);

        let canonical = request(130, 131).canonical_fingerprint().unwrap();
        let mut reversed = request(130, 131);
        reverse_participant_rows(&mut reversed.knowledge);
        assert_eq!(reversed.canonical_fingerprint().unwrap(), canonical);

        let mutations: [(&str, fn(&mut CompositeKnowledgeParticipants)); 14] = [
            ("provenance_events", |value| {
                let event = ProvenanceEvent::new(
                    value.provenance_events[0].operation_uuid,
                    EventKind::CreateNode,
                    Some(uuid7(122)),
                    value.provenance_events[0].recorded_at_micros,
                )
                .unwrap();
                value.lineage[0] = LineageRecord::new(
                    event.provenance_uuid,
                    value.lineage[0].subject_uuid,
                    value.lineage[0].subject_kind,
                    value.lineage[0].role,
                    value.lineage[0].ordinal,
                )
                .unwrap();
                value.provenance_events[0] = event;
            }),
            ("lineage", |value| {
                let row = &value.lineage[0];
                value.lineage[0] = LineageRecord::new(
                    row.provenance_uuid,
                    uuid7(123),
                    row.subject_kind,
                    row.role,
                    row.ordinal,
                )
                .unwrap();
            }),
            ("assertions", |value| {
                value.assertions[0].claim = "changed claim".into();
            }),
            ("assertion_graph_refs", |value| {
                value.assertion_graph_refs[0].graph_uuid = uuid7(123);
            }),
            ("confidence_assessments", |value| {
                value.confidence_assessments[0].assertion_uuid = uuid7(123);
            }),
            ("confidence_inputs", |value| {
                value.confidence_inputs[0].input_confidence_uuid = uuid7(123);
            }),
            ("evidence", |value| {
                value.evidence[0].source_uuid = uuid7(123);
            }),
            ("reasoning", |value| {
                value.reasoning[0].content = b"changed reason".to_vec();
            }),
            ("assertion_status", |value| {
                value.assertion_status[0].provenance_uuid = uuid7(123);
            }),
            ("assertion_supersessions", |value| {
                value.assertion_supersessions[0].provenance_uuid = uuid7(123);
            }),
            ("hypothesis_groups", |value| {
                value.hypothesis_groups[0].question_key = "question.changed".into();
            }),
            ("hypothesis_membership", |value| {
                value.hypothesis_membership[0].provenance_uuid = uuid7(123);
            }),
            ("hypothesis_selection", |value| {
                value.hypothesis_selection[0].provenance_uuid = uuid7(123);
            }),
            ("assertion_validity", |value| {
                value.assertion_validity[0].valid_to_micros = Some(101);
            }),
        ];
        for (name, mutate) in mutations {
            let mut changed = request(130, 131);
            mutate(&mut changed.knowledge);
            assert_ne!(
                changed.canonical_fingerprint().unwrap(),
                canonical,
                "{name} was omitted"
            );
        }
    }

    #[test]
    fn request_identity_actor_and_content_boundaries_are_explicit() {
        let original = request(130, 131);
        let original_fingerprint = original.canonical_fingerprint().unwrap();

        let mut changed_identity = original.clone();
        changed_identity.context.operation_uuid = OperationId(uuid7(132));
        assert_ne!(
            changed_identity.request_identity(),
            original.request_identity()
        );
        assert_eq!(
            changed_identity.canonical_fingerprint().unwrap(),
            original_fingerprint
        );

        let mut changed_actor = original.clone();
        changed_actor.context.actor_uuid = Some(uuid7(133));
        assert_ne!(
            changed_actor.canonical_fingerprint().unwrap(),
            original_fingerprint
        );

        let mut changed_content = original;
        if let CompositeGraphMutation::CreateNode { label, .. } =
            &mut changed_content.graph_mutations[0]
        {
            *label = "Researcher".into();
        }
        assert_ne!(
            changed_content.canonical_fingerprint().unwrap(),
            original_fingerprint
        );
    }

    #[test]
    fn retry_decision_matrix_is_exact_and_pre_staging() {
        let original = request(130, 131);
        let fingerprint = original.canonical_fingerprint().unwrap();
        let prior_result = vec![1_u8, 2, 3];

        assert_eq!(original.retry_decision::<Vec<u8>>(None).unwrap(), None);
        assert_eq!(
            original
                .retry_decision(Some((fingerprint, &prior_result)))
                .unwrap(),
            Some(prior_result.clone())
        );

        let mut conflict = original.clone();
        conflict.context.actor_uuid = Some(uuid7(133));
        assert_eq!(
            conflict
                .retry_decision(Some((fingerprint, &prior_result)))
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );

        let mut different_identity = original;
        different_identity.context.operation_uuid = OperationId(uuid7(132));
        assert_eq!(
            different_identity.retry_decision::<Vec<u8>>(None).unwrap(),
            None
        );
    }

    #[test]
    fn request_fingerprint_is_frozen_and_restart_stable() {
        let first = request(130, 131).canonical_fingerprint().unwrap();
        let reconstructed = request(130, 131).canonical_fingerprint().unwrap();
        assert_eq!(first, reconstructed);
        assert_eq!(
            first,
            [
                197, 130, 86, 15, 58, 98, 74, 224, 233, 185, 20, 51, 75, 151, 156, 252, 14, 2, 148,
                63, 195, 178, 52, 104, 150, 177, 213, 37, 153, 180, 121, 40,
            ]
        );
    }

    #[test]
    fn external_lineage_and_reasoning_references_reach_snapshot_validation() {
        let mut subject = request(1, 2);
        subject.knowledge.lineage.push(
            LineageRecord::new(
                uuid7(200),
                subject.knowledge.assertions[0].assertion_uuid,
                SubjectKind::Assertion,
                LineageRole::Input,
                0,
            )
            .unwrap(),
        );
        let row = &subject.knowledge.reasoning[0];
        subject.knowledge.reasoning[0] = ReasoningRecord::new(
            row.reasoning_uuid,
            row.assertion_uuid,
            row.kind,
            row.content_format,
            row.content.clone(),
            Some(uuid7(201)),
            row.provenance_uuid,
            row.recorded_at_micros,
        )
        .unwrap();

        subject.validate_request_shape().unwrap();
        subject
            .validate_ontology_and_identities(
                &crate::composite_validation::CompositeValidationSnapshot::default(),
            )
            .unwrap();
    }

    #[test]
    #[rustfmt::skip]
    fn composite_adapters_keep_malformed_and_local_invalid_rows_in_shape_validation() {
        let mut malformed = request(1, 2);
        malformed.knowledge.lineage[0] = LineageRecord::new(
            uuid7(202), uuid7(203), SubjectKind::Node, LineageRole::Input, 0).unwrap();
        malformed.knowledge.lineage[0].subject_uuid = uuid7(202);
        let error = malformed.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("lineage row is not canonical"));

        let mut duplicate_position = request(1, 2);
        duplicate_position.knowledge.lineage.extend([
            LineageRecord::new(uuid7(204), uuid7(205), SubjectKind::Node,
                LineageRole::Input, 0).unwrap(),
            LineageRecord::new(uuid7(204), uuid7(206), SubjectKind::Edge,
                LineageRole::Input, 0).unwrap(),
        ]);
        let error = duplicate_position.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error
            .to_string()
            .contains("duplicate provenance identity: role/ordinal"));

        let mut cross_assertion = request(1, 2);
        let prior = cross_assertion.knowledge.reasoning[1].reasoning_uuid;
        cross_assertion.knowledge.reasoning[0].supersedes_reasoning_uuid = Some(prior);
        let error = cross_assertion.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error
            .to_string()
            .contains("cross-assertion amendment is forbidden"));

        let mut cycle = request(1, 2);
        let assertion = cycle.knowledge.reasoning[0].assertion_uuid;
        let ids = [
            cycle.knowledge.reasoning[0].reasoning_uuid,
            cycle.knowledge.reasoning[1].reasoning_uuid,
        ];
        for index in 0..2 {
            let row = &cycle.knowledge.reasoning[index];
            cycle.knowledge.reasoning[index] = ReasoningRecord::new(
                row.reasoning_uuid,
                assertion,
                row.kind,
                row.content_format,
                row.content.clone(),
                Some(ids[1 - index]),
                row.provenance_uuid,
                row.recorded_at_micros,
            )
            .unwrap();
        }
        let error = cycle.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("cycle is forbidden"));
    }

    #[test]
    fn composite_reasoning_validates_a_long_supersession_chain() {
        const ROWS: usize = 10_000;
        let assertion_uuid = uuid7(20);
        let provenance_uuid = uuid7(1);
        let reasoning_uuid = |index: usize| {
            let mut bytes = [0_u8; 16];
            bytes[6] = 0x70;
            bytes[8] = 0x80;
            bytes[9..16].copy_from_slice(&(index as u64).to_be_bytes()[1..]);
            Uuid::from_bytes(bytes)
        };
        let rows = (0..ROWS)
            .map(|index| {
                ReasoningRecord::new(
                    reasoning_uuid(index),
                    assertion_uuid,
                    ReasoningKind::LogicalInference,
                    ReasoningContentFormat::TextPlain,
                    b"reason".to_vec(),
                    index.checked_sub(1).map(reasoning_uuid),
                    provenance_uuid,
                    index as i64,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let canonical = composite_reasoning(rows).unwrap();
        assert_eq!(canonical.len(), ROWS);
        assert_eq!(
            canonical.last().unwrap().reasoning_uuid,
            reasoning_uuid(ROWS - 1)
        );
    }

    #[test]
    fn local_only_composite_adapter_bytes_match_strict_ledgers() {
        let knowledge = full_knowledge_fixture();
        let provenance_ids = knowledge
            .provenance_events
            .iter()
            .map(|row| row.provenance_uuid)
            .collect::<HashSet<_>>();
        let strict_provenance = ProvenanceLedger::new(
            knowledge.provenance_events.clone(),
            knowledge.lineage.clone(),
        )
        .unwrap();
        let (local_lineage, external_lineage) =
            composite_lineage(&knowledge.lineage, &provenance_ids).unwrap();
        assert!(external_lineage.is_empty());
        let composite_provenance =
            ProvenanceLedger::new(knowledge.provenance_events.clone(), local_lineage).unwrap();
        assert_eq!(composite_provenance, strict_provenance);

        let strict_reasoning = ReasoningLedger::new(knowledge.reasoning.clone()).unwrap();
        let composite = composite_reasoning(knowledge.reasoning).unwrap();
        assert_eq!(composite, strict_reasoning.records);
        for row in &composite {
            assert_eq!(
                composite_reasoning_fingerprint(row).unwrap(),
                strict_reasoning
                    .record_fingerprint(row.reasoning_uuid)
                    .unwrap()
            );
        }
    }

    #[test]
    fn existing_owner_participants_are_canonical_without_repeating_owners() {
        let request = existing_owner_request();
        assert!(request.knowledge.assertions.is_empty());
        assert!(request.knowledge.confidence_assessments.is_empty());
        assert!(request.knowledge.hypothesis_groups.is_empty());

        let canonical = request.canonical_fingerprint().unwrap();
        let mut reversed = request.clone();
        reversed.knowledge.assertion_graph_refs.reverse();
        reversed.knowledge.confidence_inputs.reverse();
        reversed.knowledge.hypothesis_membership.reverse();
        reversed.knowledge.hypothesis_selection.reverse();
        assert_eq!(reversed.canonical_fingerprint().unwrap(), canonical);
        assert_eq!(
            canonical,
            [
                96, 55, 94, 210, 155, 61, 33, 239, 123, 123, 172, 250, 50, 254, 6, 191, 238, 82,
                240, 150, 131, 19, 219, 154, 115, 77, 68, 167, 67, 172, 227, 131,
            ]
        );

        let mut changed = request.clone();
        changed.knowledge.assertion_graph_refs[0].graph_uuid = uuid7(170);
        assert_ne!(changed.canonical_fingerprint().unwrap(), canonical);
        let mut changed = request.clone();
        changed.knowledge.confidence_inputs[0].input_value = Some(0.5);
        assert_ne!(changed.canonical_fingerprint().unwrap(), canonical);
        let mut changed = request.clone();
        changed.knowledge.hypothesis_membership[0].provenance_uuid = uuid7(170);
        assert_ne!(changed.canonical_fingerprint().unwrap(), canonical);
        let mut changed = request;
        changed.knowledge.hypothesis_selection[0].reasoning_uuid = uuid7(170);
        assert_ne!(changed.canonical_fingerprint().unwrap(), canonical);
    }

    #[test]
    fn existing_owner_participants_reject_malformed_duplicate_and_non_v7_rows() {
        let mut malformed = existing_owner_request();
        malformed.knowledge.assertion_graph_refs[0].contract_version = u32::MAX;
        assert_eq!(
            malformed.canonical_fingerprint().unwrap_err().code(),
            "GF_VALIDATION"
        );

        let mut duplicate = existing_owner_request();
        duplicate
            .knowledge
            .confidence_inputs
            .push(duplicate.knowledge.confidence_inputs[0].clone());
        assert_eq!(
            duplicate.canonical_fingerprint().unwrap_err().code(),
            "GF_VALIDATION"
        );

        let mut non_v7 = existing_owner_request();
        non_v7.knowledge.hypothesis_membership[0].group_uuid = Uuid::from_u128(1);
        assert_eq!(
            non_v7.canonical_fingerprint().unwrap_err().code(),
            "GF_VALIDATION"
        );
    }

    #[test]
    fn invalid_participant_fails_closed_before_retry() {
        let mut invalid = request(130, 131);
        invalid.knowledge.assertions[0].contract_version = u32::MAX;

        let fingerprint_error = invalid.canonical_fingerprint().unwrap_err();
        assert_eq!(fingerprint_error.code(), "GF_VALIDATION");
        assert!(
            fingerprint_error
                .to_string()
                .contains("invalid composite participant")
        );

        let retry_error = invalid.retry_decision::<Vec<u8>>(None).unwrap_err();
        assert_eq!(retry_error.code(), "GF_VALIDATION");
        assert!(
            retry_error
                .to_string()
                .contains("invalid composite participant")
        );
    }

    #[test]
    fn composite_request_shape_accepts_every_graph_variant_and_participant_family() {
        let mut request = request(130, 131);
        let node = uuid7(180);
        let edge = uuid7(181);
        request.graph_mutations = vec![
            CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: "Person".into(),
                properties: HashMap::from([("display_name".into(), PropValue::Str("Ada".into()))]),
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: "KNOWS".into(),
                source_uuid: node,
                target_uuid: uuid7(182),
                properties: HashMap::new(),
            },
            CompositeGraphMutation::SetNodeProperty {
                node_uuid: node,
                property: "name".into(),
                value: PropValue::Str("Grace".into()),
            },
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid: node,
                property: "name".into(),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: "since".into(),
                value: PropValue::Int(2026),
            },
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid: edge,
                property: "since".into(),
            },
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
            CompositeGraphMutation::DeleteNode { node_uuid: node },
        ];

        request.validate_request_shape().unwrap();
        assert!(
            request
                .knowledge
                .counts()
                .into_iter()
                .all(|count| count > 0)
        );
    }

    #[test]
    fn composite_request_shape_precedence_is_contract_then_context_then_limit() {
        let mut request = request(130, 131);
        request.contract_version = u32::MAX;
        request.context.operation_uuid = OperationId(Uuid::nil());
        request.context.actor_uuid = Some(Uuid::nil());
        request.knowledge = CompositeKnowledgeParticipants::default();
        request.graph_mutations = vec![
            CompositeGraphMutation::DeleteNode {
                node_uuid: Uuid::nil(),
            };
            MAX_COMPOSITE_TRANSACTION_ENTRIES + 1
        ];
        let error = request.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            "validation error: composite request has an unsupported contract version"
        );

        request.contract_version = COMPOSITE_TRANSACTION_CONTRACT_VERSION;
        let error = request.validate_request_shape().unwrap_err();
        assert_eq!(
            error.to_string(),
            "validation error: composite request has an invalid request identity"
        );

        request.context.operation_uuid = OperationId(uuid7(130));
        let error = request.validate_request_shape().unwrap_err();
        assert_eq!(
            error.to_string(),
            "validation error: composite request has an invalid actor identity"
        );

        request.context.actor_uuid = Some(uuid7(131));
        let error = request.validate_request_shape().unwrap_err();
        assert_eq!(
            error.to_string(),
            "validation error: composite transaction entry limit exceeded: 100001 > 100000"
        );
    }

    #[test]
    fn composite_request_shape_errors_precede_domain_errors_without_payloads() {
        let mut request = request(130, 131);
        request.graph_mutations[0] = CompositeGraphMutation::CreateNode {
            node_uuid: Uuid::nil(),
            label: "private-invalid-label!".into(),
            properties: HashMap::new(),
        };
        request.knowledge.assertions[0].contract_version = u32::MAX;
        request.knowledge.assertions[0].claim = "private-claim-payload".into();

        let error = request.validate_request_shape().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            "validation error: composite request has an invalid node identity"
        );
        assert!(!error.to_string().contains("private"));

        request.graph_mutations[0] = CompositeGraphMutation::CreateNode {
            node_uuid: uuid7(30),
            label: "Person".into(),
            properties: HashMap::new(),
        };
        let error = request.validate_request_shape().unwrap_err();
        assert!(error.to_string().contains("invalid composite participant"));
        assert!(!error.to_string().contains("private-claim-payload"));
    }

    #[rustfmt::skip]
    #[test]
    fn inventory_and_independent_domain_limits_are_frozen() {
        assert_eq!(COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS.len(), 14);
        assert_eq!(CompositeKnowledgeParticipants::default().counts(), [0; 14]);
        assert_eq!(gf_knowledge::MAX_KNOWLEDGE_ROWS, 1_000_000);
        assert_eq!(gf_provenance::MAX_PROVENANCE_ROWS, 1_000_000);
        assert_eq!(gf_knowledge::MAX_REASONING_CONTENT_BYTES, 65_536);
        assert_eq!(gf_knowledge::MAX_HYPOTHESIS_QUESTION_KEY_BYTES, 1_024);
        assert_eq!(
            aggregate_entry_count(usize::MAX, [1; 14])
                .unwrap_err()
                .to_string(),
            "validation error: composite transaction entry count overflow"
        );
    }

    #[test]
    #[rustfmt::skip]
    fn graph_vocabulary_is_exact() {
        let node = Uuid::nil();
        let edge = Uuid::nil();
        let request = CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext { operation_uuid: OperationId(Uuid::nil()), actor_uuid: None },
            graph_mutations: vec![
            CompositeGraphMutation::CreateNode { node_uuid: node, label: "N".into(), properties: HashMap::new() },
            CompositeGraphMutation::CreateEdge { edge_uuid: edge, rel_type: "R".into(), source_uuid: node, target_uuid: node, properties: HashMap::new() },
            CompositeGraphMutation::SetNodeProperty { node_uuid: node, property: "p".into(), value: PropValue::Int(1) },
            CompositeGraphMutation::RemoveNodeProperty { node_uuid: node, property: "p".into() },
            CompositeGraphMutation::SetEdgeProperty { edge_uuid: edge, property: "p".into(), value: PropValue::Int(1) },
            CompositeGraphMutation::RemoveEdgeProperty { edge_uuid: edge, property: "p".into() },
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
            CompositeGraphMutation::DeleteNode { node_uuid: node },
            ],
            knowledge: CompositeKnowledgeParticipants::default(),
        };
        assert_eq!(request.graph_mutations.len(), 8);
    }

    #[test]
    fn graph_mutation_order_is_caller_significant() {
        let node = Uuid::from_u128(1);
        let edge = Uuid::from_u128(2);
        let forward = vec![
            CompositeGraphMutation::DeleteNode { node_uuid: node },
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
        ];
        let reverse = vec![
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
            CompositeGraphMutation::DeleteNode { node_uuid: node },
        ];

        assert_ne!(
            canonical_graph_mutation_content_fingerprint(&forward).unwrap(),
            canonical_graph_mutation_content_fingerprint(&reverse).unwrap()
        );
    }

    #[test]
    fn property_map_iteration_order_is_not_content() {
        let first = HashMap::from([
            ("zeta".to_owned(), PropValue::Int(7)),
            ("alpha".to_owned(), PropValue::Str("value".to_owned())),
        ]);
        let mut second = HashMap::new();
        second.insert("alpha".to_owned(), PropValue::Str("value".to_owned()));
        second.insert("zeta".to_owned(), PropValue::Int(7));
        let mutation = |properties| CompositeGraphMutation::CreateNode {
            node_uuid: Uuid::from_u128(1),
            label: "Node".to_owned(),
            properties,
        };

        assert_eq!(
            canonical_graph_mutation_content_fingerprint(&[mutation(first)]).unwrap(),
            canonical_graph_mutation_content_fingerprint(&[mutation(second)]).unwrap()
        );
    }

    #[test]
    fn every_graph_mutation_field_is_fingerprint_content() {
        let one = Uuid::from_u128(1);
        let two = Uuid::from_u128(2);
        let three = Uuid::from_u128(3);
        let property_a = HashMap::from([("p".to_owned(), PropValue::Int(1))]);
        let property_b = HashMap::from([("p".to_owned(), PropValue::Int(2))]);
        let pairs = vec![
            (
                CompositeGraphMutation::CreateNode {
                    node_uuid: one,
                    label: "A".to_owned(),
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateNode {
                    node_uuid: two,
                    label: "A".to_owned(),
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateNode {
                    node_uuid: one,
                    label: "A".to_owned(),
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateNode {
                    node_uuid: one,
                    label: "B".to_owned(),
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateNode {
                    node_uuid: one,
                    label: "A".to_owned(),
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateNode {
                    node_uuid: one,
                    label: "A".to_owned(),
                    properties: property_b.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: two,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: one,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: one,
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "B".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: three,
                    target_uuid: two,
                    properties: property_a.clone(),
                },
            ),
            (
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_a.clone(),
                },
                CompositeGraphMutation::CreateEdge {
                    edge_uuid: one,
                    rel_type: "A".to_owned(),
                    source_uuid: two,
                    target_uuid: three,
                    properties: property_b.clone(),
                },
            ),
            (
                CompositeGraphMutation::DeleteNode { node_uuid: one },
                CompositeGraphMutation::DeleteNode { node_uuid: two },
            ),
            (
                CompositeGraphMutation::DeleteEdge { edge_uuid: one },
                CompositeGraphMutation::DeleteEdge { edge_uuid: two },
            ),
        ];

        for (left, right) in pairs {
            assert_ne!(
                canonical_graph_mutation_content_fingerprint(&[left]).unwrap(),
                canonical_graph_mutation_content_fingerprint(&[right]).unwrap()
            );
        }
    }

    #[test]
    fn every_property_mutation_field_is_fingerprint_content() {
        let one = Uuid::from_u128(1);
        let two = Uuid::from_u128(2);
        let pairs = vec![
            (
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: two,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
            ),
            (
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: one,
                    property: "b".to_owned(),
                    value: PropValue::Int(1),
                },
            ),
            (
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(2),
                },
            ),
            (
                CompositeGraphMutation::RemoveNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                },
                CompositeGraphMutation::RemoveNodeProperty {
                    node_uuid: two,
                    property: "a".to_owned(),
                },
            ),
            (
                CompositeGraphMutation::RemoveNodeProperty {
                    node_uuid: one,
                    property: "a".to_owned(),
                },
                CompositeGraphMutation::RemoveNodeProperty {
                    node_uuid: one,
                    property: "b".to_owned(),
                },
            ),
            (
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: two,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
            ),
            (
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: one,
                    property: "b".to_owned(),
                    value: PropValue::Int(1),
                },
            ),
            (
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(1),
                },
                CompositeGraphMutation::SetEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                    value: PropValue::Int(2),
                },
            ),
            (
                CompositeGraphMutation::RemoveEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                },
                CompositeGraphMutation::RemoveEdgeProperty {
                    edge_uuid: two,
                    property: "a".to_owned(),
                },
            ),
            (
                CompositeGraphMutation::RemoveEdgeProperty {
                    edge_uuid: one,
                    property: "a".to_owned(),
                },
                CompositeGraphMutation::RemoveEdgeProperty {
                    edge_uuid: one,
                    property: "b".to_owned(),
                },
            ),
        ];

        for (left, right) in pairs {
            assert_ne!(
                canonical_graph_mutation_content_fingerprint(&[left]).unwrap(),
                canonical_graph_mutation_content_fingerprint(&[right]).unwrap()
            );
        }
    }

    #[test]
    fn typed_property_values_have_unambiguous_canonical_tags() {
        let fingerprint = |value| {
            canonical_graph_mutation_content_fingerprint(&[
                CompositeGraphMutation::SetNodeProperty {
                    node_uuid: Uuid::from_u128(1),
                    property: "p".to_owned(),
                    value,
                },
            ])
            .unwrap()
        };
        let distinct = [
            fingerprint(PropValue::Null),
            fingerprint(PropValue::Bool(false)),
            fingerprint(PropValue::Int(0)),
            fingerprint(PropValue::Float(0.0)),
            fingerprint(PropValue::Str(String::new())),
            fingerprint(PropValue::List(Vec::new())),
        ];
        for (index, value) in distinct.iter().enumerate() {
            assert!(!distinct[..index].contains(value));
        }
        assert_eq!(
            fingerprint(PropValue::Float(0.0)),
            fingerprint(PropValue::Float(-0.0))
        );
        assert_eq!(
            fingerprint(PropValue::Float(f64::NAN)),
            fingerprint(PropValue::Float(f64::from_bits(0x7ff8_0000_0000_0001)))
        );
        assert_ne!(
            fingerprint(PropValue::List(vec![
                PropValue::List(vec![PropValue::Int(1)]),
                PropValue::Int(2),
            ])),
            fingerprint(PropValue::List(vec![
                PropValue::Int(1),
                PropValue::List(vec![PropValue::Int(2)]),
            ]))
        );
        assert_ne!(
            fingerprint(PropValue::List(vec![
                PropValue::Str("a".to_owned()),
                PropValue::Str("bc".to_owned()),
            ])),
            fingerprint(PropValue::List(vec![
                PropValue::Str("ab".to_owned()),
                PropValue::Str("c".to_owned()),
            ]))
        );
    }

    #[test]
    fn float_encoding_has_frozen_nan_and_preserves_other_bits() {
        let encoded = |value| {
            let mut writer = CanonicalWriter::new();
            encode_prop_value(&mut writer, &PropValue::Float(value)).unwrap();
            writer.finish()
        };
        let expected = |bits: u64| {
            let mut bytes = vec![3];
            bytes.extend_from_slice(&bits.to_be_bytes());
            bytes
        };

        assert_eq!(encoded(0.0), expected(0));
        assert_eq!(encoded(-0.0), expected(0));
        assert_eq!(encoded(f64::NAN), expected(0x7ff8_0000_0000_0000));
        assert_eq!(
            encoded(f64::from_bits(0xfff0_0000_0000_0001)),
            expected(0x7ff8_0000_0000_0000)
        );
        for bits in [
            (-1.5_f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            1,
        ] {
            assert_eq!(encoded(f64::from_bits(bits)), expected(bits));
        }
    }

    #[test]
    fn graph_mutation_variant_tags_are_pairwise_distinct() {
        let node = Uuid::from_u128(1);
        let edge = Uuid::from_u128(2);
        let mutations = [
            CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: String::new(),
                properties: HashMap::new(),
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: String::new(),
                source_uuid: node,
                target_uuid: node,
                properties: HashMap::new(),
            },
            CompositeGraphMutation::DeleteNode { node_uuid: node },
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
            CompositeGraphMutation::SetNodeProperty {
                node_uuid: node,
                property: String::new(),
                value: PropValue::Null,
            },
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid: node,
                property: String::new(),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: String::new(),
                value: PropValue::Null,
            },
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid: edge,
                property: String::new(),
            },
        ];
        let fingerprints = mutations
            .iter()
            .map(|mutation| {
                canonical_graph_mutation_content_fingerprint(std::slice::from_ref(mutation))
                    .unwrap()
            })
            .collect::<std::collections::HashSet<_>>();

        assert_eq!(fingerprints.len(), mutations.len());
    }

    #[test]
    fn complete_graph_mutation_vocabulary_has_frozen_digest() {
        let node = Uuid::from_u128(1);
        let edge = Uuid::from_u128(2);
        let properties = HashMap::from([
            ("bool".to_owned(), PropValue::Bool(true)),
            ("float".to_owned(), PropValue::Float(-1.5)),
            ("int".to_owned(), PropValue::Int(-7)),
            (
                "list".to_owned(),
                PropValue::List(vec![PropValue::Null, PropValue::Str("x".to_owned())]),
            ),
            ("null".to_owned(), PropValue::Null),
            ("string".to_owned(), PropValue::Str("value".to_owned())),
        ]);
        let mutations = vec![
            CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: "Node".to_owned(),
                properties,
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: "REL".to_owned(),
                source_uuid: node,
                target_uuid: node,
                properties: HashMap::new(),
            },
            CompositeGraphMutation::SetNodeProperty {
                node_uuid: node,
                property: "p".to_owned(),
                value: PropValue::List(vec![PropValue::Int(1)]),
            },
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid: node,
                property: "p".to_owned(),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: "p".to_owned(),
                value: PropValue::Float(f64::NAN),
            },
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid: edge,
                property: "p".to_owned(),
            },
            CompositeGraphMutation::DeleteEdge { edge_uuid: edge },
            CompositeGraphMutation::DeleteNode { node_uuid: node },
        ];

        assert_eq!(
            canonical_graph_mutation_content_fingerprint(&mutations).unwrap(),
            [
                0x26, 0xaf, 0xa4, 0xdc, 0x8d, 0xa0, 0x98, 0xf2, 0x73, 0x30, 0x47, 0x78, 0x56, 0xb3,
                0xa9, 0x35, 0x71, 0x4f, 0xef, 0x05, 0x7c, 0x39, 0x64, 0x78, 0x4b, 0x11, 0x2e, 0xd5,
                0xc7, 0x39, 0xeb, 0x56,
            ]
        );
    }

    #[test]
    #[rustfmt::skip]
    fn aggregate_limit_accepts_exactly_one_hundred_thousand() {
        assert_eq!(bounded_entry_count(MAX_COMPOSITE_TRANSACTION_ENTRIES, [0; 14]).unwrap(), MAX_COMPOSITE_TRANSACTION_ENTRIES);
        let error = bounded_entry_count(MAX_COMPOSITE_TRANSACTION_ENTRIES, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err();
        assert_eq!(error.to_string(), "validation error: composite transaction entry limit exceeded: 100001 > 100000");
    }
}
