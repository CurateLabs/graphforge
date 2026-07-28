use std::collections::BTreeSet;

use gf_core::{ApiErrorCode, GfError, OntologyMode};
use gf_knowledge::GraphObjectKind;
use gf_provenance::SubjectKind;
use uuid::Uuid;

use crate::composite_transaction::{CompositeGraphMutation, CompositeTransactionRequest};

/// Existing identities and ontology used by composite pre-staging authorization.
///
/// Callers (and later `GraphForge` publication) supply the pinned generation's
/// known UUIDs. This snapshot is read-only validation input; authorization never
/// mutates storage through it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[rustfmt::skip]
pub struct CompositeValidationSnapshot {
    /// Ontology mode and declared types for the pinned generation.
    pub ontology: CompositeOntologySnapshot,
    /// Existing node identities.
    pub nodes: BTreeSet<Uuid>,
    /// Existing edge identities.
    pub edges: BTreeSet<Uuid>,
    /// Existing provenance event identities.
    pub provenance: BTreeSet<Uuid>,
    /// Existing lineage identities.
    pub lineage: BTreeSet<Uuid>,
    /// Existing assertion identities.
    pub assertions: BTreeSet<Uuid>,
    /// Existing confidence assessment identities.
    pub confidence: BTreeSet<Uuid>,
    /// Existing evidence identities.
    pub evidence: BTreeSet<Uuid>,
    /// Existing reasoning identities.
    pub reasoning: BTreeSet<Uuid>,
    /// Existing assertion-status event identities.
    pub status_events: BTreeSet<Uuid>,
    /// Existing supersession identities.
    pub supersessions: BTreeSet<Uuid>,
    /// Existing hypothesis group identities.
    pub hypothesis_groups: BTreeSet<Uuid>,
    /// Existing hypothesis membership event identities.
    pub membership_events: BTreeSet<Uuid>,
    /// Existing hypothesis selection event identities.
    pub selection_events: BTreeSet<Uuid>,
    /// Existing assertion validity event identities.
    pub validity_events: BTreeSet<Uuid>,
    /// Existing algorithm-run identities.
    pub algorithm_runs: BTreeSet<Uuid>,
    /// Existing belief-projection attachment identities.
    pub belief_projection_attachments: BTreeSet<Uuid>,
}

impl CompositeValidationSnapshot {
    #[rustfmt::skip]
    fn contains_any(&self, uuid: Uuid) -> bool {
        [
            &self.nodes, &self.edges, &self.provenance, &self.lineage,
            &self.assertions, &self.confidence, &self.evidence, &self.reasoning,
            &self.status_events, &self.supersessions, &self.hypothesis_groups,
            &self.membership_events, &self.selection_events, &self.validity_events, &self.algorithm_runs,
            &self.belief_projection_attachments,
        ]
        .iter()
        .any(|identities| identities.contains(&uuid))
    }
}

/// Ontology mode and declared type names visible to composite validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeOntologySnapshot {
    /// Exploratory or strict ontology mode.
    pub mode: OntologyMode,
    /// Declared entity type names when mode is strict.
    pub entity_types: BTreeSet<String>,
    /// Declared relationship type names when mode is strict.
    pub relation_types: BTreeSet<String>,
}

impl Default for CompositeOntologySnapshot {
    fn default() -> Self {
        Self {
            mode: OntologyMode::Exploratory,
            entity_types: BTreeSet::new(),
            relation_types: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
#[rustfmt::skip]
pub(crate) struct RequestIdentities {
    pub(crate) occupied: BTreeSet<Uuid>, pub(crate) nodes: BTreeSet<Uuid>,
    pub(crate) edges: BTreeSet<Uuid>, pub(crate) provenance: BTreeSet<Uuid>,
    pub(crate) assertions: BTreeSet<Uuid>, pub(crate) confidence: BTreeSet<Uuid>,
    pub(crate) evidence: BTreeSet<Uuid>, pub(crate) reasoning: BTreeSet<Uuid>,
    pub(crate) status_events: BTreeSet<Uuid>, pub(crate) hypothesis_groups: BTreeSet<Uuid>,
}

impl RequestIdentities {
    fn insert(
        &mut self,
        uuid: Uuid,
        snapshot: &CompositeValidationSnapshot,
    ) -> Result<(), GfError> {
        if snapshot.contains_any(uuid) || !self.occupied.insert(uuid) {
            return Err(GfError::Api {
                code: ApiErrorCode::IdentityConflict,
                message: "composite request reuses an occupied identity".into(),
            });
        }
        Ok(())
    }
}

impl CompositeTransactionRequest {
    pub(crate) fn validate_ontology_and_identities(
        &self,
        snapshot: &CompositeValidationSnapshot,
    ) -> Result<RequestIdentities, GfError> {
        self.validate_request_shape()?;
        self.validate_ontology(&snapshot.ontology)?;
        self.collect_identities(snapshot)
    }

    fn validate_ontology(&self, ontology: &CompositeOntologySnapshot) -> Result<(), GfError> {
        if ontology.mode != OntologyMode::Strict {
            return Ok(());
        }
        for mutation in &self.graph_mutations {
            if let CompositeGraphMutation::CreateNode { label, .. } = mutation
                && !ontology.entity_types.contains(label)
            {
                return Err(GfError::Ontology(
                    "composite request uses an undeclared entity type".into(),
                ));
            }
        }
        for mutation in &self.graph_mutations {
            if let CompositeGraphMutation::CreateEdge { rel_type, .. } = mutation
                && !ontology.relation_types.contains(rel_type)
            {
                return Err(GfError::Ontology(
                    "composite request uses an undeclared relationship type".into(),
                ));
            }
        }
        Ok(())
    }

    #[rustfmt::skip]
    fn collect_identities(
        &self,
        snapshot: &CompositeValidationSnapshot,
    ) -> Result<RequestIdentities, GfError> {
        let mut result = RequestIdentities::default();
        for mutation in &self.graph_mutations {
            match mutation {
                CompositeGraphMutation::CreateNode { node_uuid, .. } => {
                    result.insert(*node_uuid, snapshot)?;
                    result.nodes.insert(*node_uuid);
                }
                CompositeGraphMutation::CreateEdge { edge_uuid, .. } => {
                    result.insert(*edge_uuid, snapshot)?;
                    result.edges.insert(*edge_uuid);
                }
                _ => {}
            }
        }
        macro_rules! collect {
            ($rows:expr, $field:ident, $uuid:ident) => {
                for row in $rows {
                    result.insert(row.$uuid, snapshot)?;
                    result.$field.insert(row.$uuid);
                }
            };
            ($rows:expr, $uuid:ident) => {
                for row in $rows {
                    result.insert(row.$uuid, snapshot)?;
                }
            };
        }
        collect!(&self.knowledge.provenance_events, provenance, provenance_uuid);
        collect!(&self.knowledge.lineage, lineage_uuid);
        collect!(&self.knowledge.assertions, assertions, assertion_uuid);
        collect!(&self.knowledge.confidence_assessments, confidence, confidence_uuid);
        collect!(&self.knowledge.evidence, evidence, evidence_uuid);
        collect!(&self.knowledge.reasoning, reasoning, reasoning_uuid);
        collect!(&self.knowledge.assertion_status, status_events, status_event_uuid);
        collect!(&self.knowledge.assertion_supersessions, supersession_uuid);
        collect!(&self.knowledge.hypothesis_groups, hypothesis_groups, group_uuid);
        collect!(&self.knowledge.hypothesis_membership, membership_event_uuid);
        collect!(&self.knowledge.hypothesis_selection, selection_event_uuid);
        collect!(&self.knowledge.assertion_validity, validity_event_uuid);
        Ok(result)
    }

    /// Validate every M20/M21 participant reference against existing or
    /// same-request identities before any staging.
    ///
    /// Provenance/assertion families run before epistemic event families so
    /// multi-defect requests always surface the documented first error.
    pub(crate) fn validate_participant_references(
        &self,
        snapshot: &CompositeValidationSnapshot,
        request: &RequestIdentities,
    ) -> Result<(), GfError> {
        self.validate_provenance_assertion_references(snapshot, request)?;
        self.validate_epistemic_event_references(snapshot, request)?;
        Ok(())
    }

    /// Validate graph then M20/M21 participant cross-references before staging.
    ///
    /// Graph endpoint/kind checks precede participant-family checks so
    /// multi-defect requests keep the documented reference precedence.
    pub(crate) fn validate_graph_and_participant_references(
        &self,
        snapshot: &CompositeValidationSnapshot,
        request: &RequestIdentities,
    ) -> Result<(), GfError> {
        self.validate_graph_references(snapshot, request)?;
        self.validate_participant_references(snapshot, request)?;
        Ok(())
    }

    pub(crate) fn validate_provenance_assertion_references(
        &self,
        snapshot: &CompositeValidationSnapshot,
        request: &RequestIdentities,
    ) -> Result<(), GfError> {
        let present = |existing: &BTreeSet<Uuid>, added: &BTreeSet<Uuid>, uuid| {
            existing.contains(&uuid) || added.contains(&uuid)
        };
        let provenance = |uuid| present(&snapshot.provenance, &request.provenance, uuid);
        let assertion = |uuid| present(&snapshot.assertions, &request.assertions, uuid);
        let confidence = |uuid| present(&snapshot.confidence, &request.confidence, uuid);
        let reasoning = |uuid| present(&snapshot.reasoning, &request.reasoning, uuid);
        let node = |uuid| snapshot.nodes.contains(&uuid) || request.nodes.contains(&uuid);
        let edge = |uuid| snapshot.edges.contains(&uuid) || request.edges.contains(&uuid);

        for row in &self.knowledge.provenance_events {
            if row.operation_uuid != self.context.operation_uuid.0
                || row.actor_uuid != self.context.actor_uuid
            {
                return Err(identity_conflict(
                    "composite provenance identity does not match request context",
                ));
            }
        }
        for row in &self.knowledge.lineage {
            require(
                provenance(row.provenance_uuid),
                "composite lineage provenance is missing",
            )?;
            let subject_exists = match row.subject_kind {
                SubjectKind::Node => node(row.subject_uuid),
                SubjectKind::Edge => edge(row.subject_uuid),
                SubjectKind::Assertion => assertion(row.subject_uuid),
                SubjectKind::EvidenceLink => {
                    present(&snapshot.evidence, &request.evidence, row.subject_uuid)
                }
                SubjectKind::ConfidenceAssessment => confidence(row.subject_uuid),
                SubjectKind::AlgorithmRun => snapshot.algorithm_runs.contains(&row.subject_uuid),
                SubjectKind::BeliefProjectionAttachment => snapshot
                    .belief_projection_attachments
                    .contains(&row.subject_uuid),
            };
            require(subject_exists, "composite lineage subject is missing")?;
        }
        for row in &self.knowledge.assertions {
            require(
                provenance(row.provenance_uuid),
                "composite assertion provenance is missing",
            )?;
        }
        for row in &self.knowledge.assertion_graph_refs {
            require(
                assertion(row.assertion_uuid),
                "composite graph reference assertion is missing",
            )?;
        }
        for row in &self.knowledge.confidence_assessments {
            require(
                assertion(row.assertion_uuid),
                "composite confidence assertion is missing",
            )?;
            require(
                provenance(row.provenance_uuid),
                "composite confidence provenance is missing",
            )?;
        }
        for row in &self.knowledge.confidence_inputs {
            require(
                confidence(row.confidence_uuid),
                "composite confidence owner is missing",
            )?;
            require(
                confidence(row.input_confidence_uuid),
                "composite confidence input is missing",
            )?;
        }
        for row in &self.knowledge.evidence {
            require(
                assertion(row.assertion_uuid),
                "composite evidence assertion is missing",
            )?;
            require(
                provenance(row.provenance_uuid),
                "composite evidence provenance is missing",
            )?;
        }
        for row in &self.knowledge.reasoning {
            require(
                assertion(row.assertion_uuid),
                "composite reasoning assertion is missing",
            )?;
            if let Some(uuid) = row.supersedes_reasoning_uuid {
                require(reasoning(uuid), "composite prior reasoning is missing")?;
            }
            require(
                provenance(row.provenance_uuid),
                "composite reasoning provenance is missing",
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate_graph_references(
        &self,
        snapshot: &CompositeValidationSnapshot,
        request: &RequestIdentities,
    ) -> Result<(), GfError> {
        let nodes = |uuid| snapshot.nodes.contains(&uuid) || request.nodes.contains(&uuid);
        let edges = |uuid| snapshot.edges.contains(&uuid) || request.edges.contains(&uuid);
        for mutation in &self.graph_mutations {
            if let CompositeGraphMutation::CreateEdge { source_uuid, .. } = mutation
                && !nodes(*source_uuid)
            {
                return Err(reference_missing(
                    "composite edge source_uuid does not resolve to a node",
                ));
            }
        }
        for mutation in &self.graph_mutations {
            if let CompositeGraphMutation::CreateEdge { target_uuid, .. } = mutation
                && !nodes(*target_uuid)
            {
                return Err(reference_missing(
                    "composite edge target_uuid does not resolve to a node",
                ));
            }
        }
        for mutation in &self.graph_mutations {
            let target = match mutation {
                CompositeGraphMutation::DeleteNode { node_uuid }
                | CompositeGraphMutation::SetNodeProperty { node_uuid, .. }
                | CompositeGraphMutation::RemoveNodeProperty { node_uuid, .. } => Some(*node_uuid),
                _ => None,
            };
            if target.is_some_and(|uuid| !nodes(uuid)) {
                return Err(reference_missing(
                    "composite node mutation target does not resolve to a node",
                ));
            }
        }
        for mutation in &self.graph_mutations {
            let target = match mutation {
                CompositeGraphMutation::DeleteEdge { edge_uuid }
                | CompositeGraphMutation::SetEdgeProperty { edge_uuid, .. }
                | CompositeGraphMutation::RemoveEdgeProperty { edge_uuid, .. } => Some(*edge_uuid),
                _ => None,
            };
            if target.is_some_and(|uuid| !edges(uuid)) {
                return Err(reference_missing(
                    "composite edge mutation target does not resolve to an edge",
                ));
            }
        }
        for row in &self.knowledge.assertion_graph_refs {
            let exists = match row.graph_kind {
                GraphObjectKind::Node => nodes(row.graph_uuid),
                GraphObjectKind::Edge => edges(row.graph_uuid),
            };
            if !exists {
                return Err(reference_missing(
                    "composite graph reference does not resolve to its declared kind",
                ));
            }
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one linear pass keeps fixed epistemic family and field precedence auditable"
    )]
    pub(crate) fn validate_epistemic_event_references(
        &self,
        snapshot: &CompositeValidationSnapshot,
        request: &RequestIdentities,
    ) -> Result<(), GfError> {
        let present = |existing: &BTreeSet<Uuid>, added: &BTreeSet<Uuid>, uuid| {
            existing.contains(&uuid) || added.contains(&uuid)
        };
        let provenance = |uuid| present(&snapshot.provenance, &request.provenance, uuid);
        let assertion = |uuid| present(&snapshot.assertions, &request.assertions, uuid);
        let confidence = |uuid| present(&snapshot.confidence, &request.confidence, uuid);
        let reasoning = |uuid| present(&snapshot.reasoning, &request.reasoning, uuid);
        let status = |uuid| present(&snapshot.status_events, &request.status_events, uuid);
        let group = |uuid| {
            present(
                &snapshot.hypothesis_groups,
                &request.hypothesis_groups,
                uuid,
            )
        };

        for row in &self.knowledge.assertion_status {
            require(
                assertion(row.assertion_uuid),
                "composite status assertion is missing",
            )?;
            if let Some(uuid) = row.confidence_uuid {
                require(confidence(uuid), "composite status confidence is missing")?;
            }
            if let Some(uuid) = row.reasoning_uuid {
                require(reasoning(uuid), "composite status reasoning is missing")?;
            }
            require(
                provenance(row.provenance_uuid),
                "composite status provenance is missing",
            )?;
        }
        for row in &self.knowledge.assertion_supersessions {
            require(
                assertion(row.prior_assertion_uuid),
                "composite prior assertion is missing",
            )?;
            require(
                assertion(row.replacement_assertion_uuid),
                "composite replacement assertion is missing",
            )?;
            require(
                status(row.status_event_uuid),
                "composite supersession status is missing",
            )?;
            require(
                reasoning(row.reasoning_uuid),
                "composite supersession reasoning is missing",
            )?;
            require(
                provenance(row.provenance_uuid),
                "composite supersession provenance is missing",
            )?;
        }
        for row in &self.knowledge.hypothesis_groups {
            require(
                provenance(row.provenance_uuid),
                "composite hypothesis provenance is missing",
            )?;
        }
        for row in &self.knowledge.hypothesis_membership {
            require_operation(row.operation_uuid, self.context.operation_uuid.0)?;
            require(
                group(row.group_uuid),
                "composite membership group is missing",
            )?;
            require(
                assertion(row.assertion_uuid),
                "composite membership assertion is missing",
            )?;
            require(
                reasoning(row.reasoning_uuid),
                "composite membership reasoning is missing",
            )?;
            require(
                provenance(row.provenance_uuid),
                "composite membership provenance is missing",
            )?;
        }
        for row in &self.knowledge.hypothesis_selection {
            require_operation(row.operation_uuid, self.context.operation_uuid.0)?;
            require(
                group(row.group_uuid),
                "composite selection group is missing",
            )?;
            if let Some(uuid) = row.selected_assertion_uuid {
                require(assertion(uuid), "composite selected assertion is missing")?;
            }
            require(
                reasoning(row.reasoning_uuid),
                "composite selection reasoning is missing",
            )?;
            require(
                provenance(row.provenance_uuid),
                "composite selection provenance is missing",
            )?;
        }
        for row in &self.knowledge.assertion_validity {
            require(
                assertion(row.assertion_uuid),
                "composite validity assertion is missing",
            )?;
            if let Some(uuid) = row.reasoning_uuid {
                require(reasoning(uuid), "composite validity reasoning is missing")?;
            }
            require(
                provenance(row.provenance_uuid),
                "composite validity provenance is missing",
            )?;
        }
        Ok(())
    }
}

fn require(present: bool, message: &'static str) -> Result<(), GfError> {
    if present {
        Ok(())
    } else {
        Err(reference_missing(message))
    }
}

fn identity_conflict(message: &'static str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::IdentityConflict,
        message: message.into(),
    }
}

fn require_operation(actual: Uuid, expected: Uuid) -> Result<(), GfError> {
    if actual == expected {
        Ok(())
    } else {
        Err(identity_conflict(
            "composite participant operation does not match request identity",
        ))
    }
}

fn reference_missing(message: &'static str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::composite_transaction::{
        COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeKnowledgeParticipants,
    };
    use crate::{OperationId, WriteContext};
    use gf_core::PropValue;
    use gf_knowledge::{AssertionGraphRef, AssertionGraphRole};

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    #[rustfmt::skip]
    fn request(mutations: Vec<CompositeGraphMutation>) -> CompositeTransactionRequest {
        CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext { operation_uuid: OperationId(uuid7(1)), actor_uuid: None },
            graph_mutations: mutations,
            knowledge: CompositeKnowledgeParticipants::default(),
        }
    }

    fn node(uuid: Uuid, label: &str) -> CompositeGraphMutation {
        CompositeGraphMutation::CreateNode {
            node_uuid: uuid,
            label: label.into(),
            properties: HashMap::new(),
        }
    }

    fn edge(uuid: Uuid, source: Uuid, target: Uuid) -> CompositeGraphMutation {
        CompositeGraphMutation::CreateEdge {
            edge_uuid: uuid,
            rel_type: "KNOWS".into(),
            source_uuid: source,
            target_uuid: target,
            properties: HashMap::new(),
        }
    }

    fn target_mutations(node_uuid: Uuid, edge_uuid: Uuid) -> Vec<CompositeGraphMutation> {
        vec![
            CompositeGraphMutation::DeleteNode { node_uuid },
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property: "p".into(),
                value: PropValue::Int(1),
            },
            CompositeGraphMutation::RemoveNodeProperty {
                node_uuid,
                property: "p".into(),
            },
            CompositeGraphMutation::DeleteEdge { edge_uuid },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property: "p".into(),
                value: PropValue::Int(1),
            },
            CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid,
                property: "p".into(),
            },
        ]
    }

    fn graph_ref(graph_uuid: Uuid, graph_kind: GraphObjectKind) -> AssertionGraphRef {
        AssertionGraphRef::new(
            uuid7(match graph_kind {
                GraphObjectKind::Node => 240,
                GraphObjectKind::Edge => 241,
            }),
            graph_uuid,
            graph_kind,
            AssertionGraphRole::Subject,
            0,
        )
        .unwrap()
    }

    fn assert_not_found(result: Result<(), GfError>, expected: &str) {
        assert_reference_error(result, ApiErrorCode::NotFound, expected);
    }

    #[rustfmt::skip]
    fn assert_conflict(result: Result<RequestIdentities, GfError>) {
        match result.unwrap_err() {
            GfError::Api { code, message } => {
                assert_eq!(code, ApiErrorCode::IdentityConflict);
                assert_eq!(message.as_str(), "composite request reuses an occupied identity");
            }
            error => panic!("expected identity conflict, got {error}"),
        }
    }

    #[rustfmt::skip]
    fn reference_request() -> CompositeTransactionRequest {
        let mut subject = request(vec![node(uuid7(230), "Person")]);
        subject.knowledge = crate::composite_transaction::tests::full_knowledge_fixture();
        let operation = subject.context.operation_uuid.0;
        for row in &mut subject.knowledge.provenance_events {
            *row = gf_provenance::ProvenanceEvent::new(operation, row.event_kind, None, row.recorded_at_micros).unwrap();
        }
        subject.knowledge.provenance_events.truncate(1);
        let provenance = subject.knowledge.provenance_events[0].provenance_uuid;
        let assertion = subject.knowledge.assertions[0].assertion_uuid;
        let confidence = subject.knowledge.confidence_assessments[0].confidence_uuid;
        let later_reasoning = subject.knowledge.reasoning[1].reasoning_uuid;
        subject.knowledge.lineage.truncate(1);
        let lineage = &subject.knowledge.lineage[0];
        subject.knowledge.lineage[0] = gf_provenance::LineageRecord::new(
            provenance, assertion, SubjectKind::Assertion, lineage.role, lineage.ordinal).unwrap();
        for row in &mut subject.knowledge.assertions {
            *row = gf_knowledge::Assertion::new(
                row.assertion_uuid, row.claim.clone(), provenance, row.recorded_at_micros).unwrap();
        }
        for (graph_ref, owner) in subject.knowledge.assertion_graph_refs.iter_mut().zip(&subject.knowledge.assertions) {
            *graph_ref = gf_knowledge::AssertionGraphRef::new(
                owner.assertion_uuid, uuid7(230), GraphObjectKind::Node, graph_ref.role, graph_ref.ordinal).unwrap();
        }
        for row in &mut subject.knowledge.confidence_assessments {
            *row = gf_knowledge::ConfidenceAssessment::new(
                row.confidence_uuid, assertion, row.policy, row.value, provenance, row.recorded_at_micros).unwrap();
        }
        subject.knowledge.confidence_inputs.truncate(1);
        let input = &subject.knowledge.confidence_inputs[0];
        subject.knowledge.confidence_inputs[0] = gf_knowledge::ConfidenceInput::new(
            confidence, subject.knowledge.confidence_assessments[1].confidence_uuid,
            input.input_value, input.ordinal).unwrap();
        subject.knowledge.evidence.truncate(1);
        let evidence = &subject.knowledge.evidence[0];
        subject.knowledge.evidence[0] = gf_knowledge::EvidenceLink::new(
            evidence.evidence_uuid, assertion, evidence.source_uuid, gf_knowledge::EvidenceSourceKind::Observation,
            evidence.role, evidence.weight, provenance, evidence.recorded_at_micros).unwrap();
        for (index, row) in subject.knowledge.reasoning.iter_mut().enumerate() {
            *row = gf_knowledge::ReasoningRecord::new(
                row.reasoning_uuid, assertion, row.kind, row.content_format, row.content.clone(),
                (index == 0).then_some(later_reasoning), provenance, row.recorded_at_micros).unwrap();
        }
        subject.knowledge.assertion_status.clear();
        subject.knowledge.assertion_supersessions.clear();
        subject.knowledge.hypothesis_groups.clear();
        subject.knowledge.hypothesis_membership.clear();
        subject.knowledge.hypothesis_selection.clear();
        subject.knowledge.assertion_validity.clear();
        subject
    }

    #[rustfmt::skip]
    fn epistemic_request() -> CompositeTransactionRequest {
        let mut subject = request(Vec::new());
        subject.knowledge = crate::composite_transaction::tests::full_knowledge_fixture();
        let operation = subject.context.operation_uuid.0;
        let assertions = [
            subject.knowledge.assertions[0].assertion_uuid,
            subject.knowledge.assertions[1].assertion_uuid,
        ];
        subject.knowledge.assertion_supersessions.truncate(1);
        subject.knowledge.assertion_supersessions[0].prior_assertion_uuid = assertions[0];
        subject.knowledge.assertion_supersessions[0].replacement_assertion_uuid = assertions[1];
        for row in &mut subject.knowledge.hypothesis_membership { row.operation_uuid = operation; }
        for row in &mut subject.knowledge.hypothesis_selection { row.operation_uuid = operation; }
        subject
    }

    fn assert_reference_error(result: Result<(), GfError>, code: ApiErrorCode, message: &str) {
        match result.unwrap_err() {
            GfError::Api {
                code: actual,
                message: actual_message,
            } => {
                assert_eq!(actual, code);
                assert_eq!(actual_message, message);
            }
            error => panic!("expected reference error, got {error}"),
        }
    }

    #[test]
    #[rustfmt::skip]
    fn strict_ontology_precedes_identity_and_reports_entity_before_relationship() {
        let occupied = uuid7(2);
        let mut snapshot = CompositeValidationSnapshot::default();
        snapshot.ontology.mode = OntologyMode::Strict;
        snapshot.nodes.insert(occupied);
        let subject = request(vec![
            CompositeGraphMutation::CreateEdge {
                edge_uuid: uuid7(3),
                rel_type: "KNOWS".into(),
                source_uuid: occupied,
                target_uuid: occupied,
                properties: HashMap::new(),
            },
            node(occupied, "Person"),
        ]);

        let GfError::Ontology(message) = subject.validate_ontology_and_identities(&snapshot).unwrap_err()
            else { panic!("expected ontology error") };
        assert_eq!(message, "composite request uses an undeclared entity type");
        snapshot.ontology.entity_types.insert("Person".into());
        let GfError::Ontology(message) = subject.validate_ontology_and_identities(&snapshot).unwrap_err()
            else { panic!("expected ontology error") };
        assert_eq!(message, "composite request uses an undeclared relationship type");
        let undeclared = request(vec![node(uuid7(4), "not declared")]);
        for mode in [OntologyMode::Exploratory, OntologyMode::Advisory] {
            let mut snapshot = CompositeValidationSnapshot::default();
            snapshot.ontology.mode = mode;
            assert!(
                undeclared
                    .validate_ontology_and_identities(&snapshot)
                    .is_ok()
            );
        }
    }

    #[test]
    #[rustfmt::skip]
    fn collisions_are_stable_across_existing_and_same_request_families() {
        let identity = uuid7(200);
        let mut snapshots = Vec::new();
        macro_rules! occupied_in {
            ($field:ident) => {{
                let mut snapshot = CompositeValidationSnapshot::default();
                snapshot.$field.insert(identity);
                snapshots.push(snapshot);
            }};
        }
        occupied_in!(nodes); occupied_in!(edges); occupied_in!(provenance); occupied_in!(lineage);
        occupied_in!(assertions); occupied_in!(confidence); occupied_in!(evidence); occupied_in!(reasoning);
        occupied_in!(status_events); occupied_in!(supersessions); occupied_in!(hypothesis_groups);
        occupied_in!(membership_events); occupied_in!(selection_events); occupied_in!(validity_events);
        occupied_in!(algorithm_runs); occupied_in!(belief_projection_attachments);
        for snapshot in snapshots {
            assert_conflict(
                request(vec![node(identity, "Person")]).validate_ontology_and_identities(&snapshot),
            );
        }

        for family in 0..12 {
            let mut cross_family = request(vec![node(identity, "Person")]);
            cross_family.knowledge = crate::composite_transaction::tests::full_knowledge_fixture();
            match family {
                0 => cross_family.knowledge.provenance_events[0].provenance_uuid = identity, 1 => cross_family.knowledge.lineage[0].lineage_uuid = identity,
                2 => cross_family.knowledge.assertions[0].assertion_uuid = identity, 3 => cross_family.knowledge.confidence_assessments[0].confidence_uuid = identity,
                4 => cross_family.knowledge.evidence[0].evidence_uuid = identity, 5 => cross_family.knowledge.reasoning[0].reasoning_uuid = identity,
                6 => cross_family.knowledge.assertion_status[0].status_event_uuid = identity, 7 => cross_family.knowledge.assertion_supersessions[0].supersession_uuid = identity,
                8 => cross_family.knowledge.hypothesis_groups[0].group_uuid = identity, 9 => cross_family.knowledge.hypothesis_membership[0].membership_event_uuid = identity,
                10 => cross_family.knowledge.hypothesis_selection[0].selection_event_uuid = identity, 11 => cross_family.knowledge.assertion_validity[0].validity_event_uuid = identity,
                _ => unreachable!(),
            }
            assert_conflict(cross_family.collect_identities(&CompositeValidationSnapshot::default()));
        }
    }

    #[test]
    fn provenance_assertion_forward_and_optional_references_are_order_independent() {
        let mut subject = reference_request();
        let snapshot = CompositeValidationSnapshot::default();
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert!(
            subject
                .validate_provenance_assertion_references(&snapshot, &identities)
                .is_ok()
        );
        for row in &mut subject.knowledge.reasoning {
            row.supersedes_reasoning_uuid = None;
        }
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert!(
            subject
                .validate_provenance_assertion_references(&snapshot, &identities)
                .is_ok()
        );
    }

    #[test]
    #[rustfmt::skip]
    fn snapshot_and_request_references_accept_every_lineage_subject_kind() {
        let provenance = uuid7(241); let assertion = uuid7(242); let evidence = uuid7(243);
        let confidence = uuid7(244); let input_confidence = uuid7(245); let reasoning = uuid7(246);
        let edge = uuid7(247); let algorithm = uuid7(248); let projection = uuid7(249);
        let mut snapshot = CompositeValidationSnapshot::default();
        snapshot.provenance.insert(provenance); snapshot.assertions.insert(assertion);
        snapshot.evidence.insert(evidence); snapshot.confidence.extend([confidence, input_confidence]);
        snapshot.reasoning.insert(reasoning); snapshot.edges.insert(edge);
        snapshot.algorithm_runs.insert(algorithm); snapshot.belief_projection_attachments.insert(projection);
        let cases = [
            (SubjectKind::Node, uuid7(230)), (SubjectKind::Edge, edge),
            (SubjectKind::Assertion, assertion), (SubjectKind::EvidenceLink, evidence),
            (SubjectKind::ConfidenceAssessment, confidence), (SubjectKind::AlgorithmRun, algorithm),
            (SubjectKind::BeliefProjectionAttachment, projection),
        ];
        for (kind, subject_uuid) in cases {
            let mut subject = reference_request();
            for row in &mut subject.knowledge.assertions { row.provenance_uuid = provenance; }
            let graph_ref = &subject.knowledge.assertion_graph_refs[0];
            subject.knowledge.assertion_graph_refs.push(gf_knowledge::AssertionGraphRef::new(
                assertion, graph_ref.graph_uuid, graph_ref.graph_kind, graph_ref.role, 0).unwrap());
            for row in &mut subject.knowledge.confidence_assessments { row.assertion_uuid = assertion; row.provenance_uuid = provenance; }
            subject.knowledge.confidence_inputs.push(gf_knowledge::ConfidenceInput::new(
                confidence, input_confidence, None, 0).unwrap());
            subject.knowledge.evidence[0].assertion_uuid = assertion; subject.knowledge.evidence[0].provenance_uuid = provenance;
            for row in &mut subject.knowledge.reasoning { row.assertion_uuid = assertion; row.supersedes_reasoning_uuid = Some(reasoning); row.provenance_uuid = provenance; }
            let row = &subject.knowledge.lineage[0];
            subject.knowledge.lineage[0] = gf_provenance::LineageRecord::new(
                provenance, subject_uuid, kind, row.role, row.ordinal).unwrap();
            let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
            subject.validate_provenance_assertion_references(&snapshot, &identities).unwrap();
        }
    }

    #[test]
    #[rustfmt::skip]
    fn every_owned_reference_rejects_a_wrong_identity_family() {
        let wrong = uuid7(240);
        for case in 0..19 {
            let mut subject = reference_request();
            let mut snapshot = CompositeValidationSnapshot::default();
            snapshot.status_events.insert(wrong);
            let expected = match case {
                0 => { subject.knowledge.lineage[0].provenance_uuid = wrong; "composite lineage provenance is missing" }
                1 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::Node; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                2 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::Edge; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                3 => { subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                4 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::EvidenceLink; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                5 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::ConfidenceAssessment; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                6 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::AlgorithmRun; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                7 => { subject.knowledge.lineage[0].subject_kind = SubjectKind::BeliefProjectionAttachment; subject.knowledge.lineage[0].subject_uuid = wrong; "composite lineage subject is missing" }
                8 => { subject.knowledge.assertions[0].provenance_uuid = wrong; "composite assertion provenance is missing" }
                9 => {
                    let row = &subject.knowledge.assertion_graph_refs[0];
                    subject.knowledge.assertion_graph_refs.push(gf_knowledge::AssertionGraphRef::new(
                        wrong, row.graph_uuid, row.graph_kind, row.role, 0).unwrap());
                    "composite graph reference assertion is missing"
                }
                10 => { subject.knowledge.confidence_assessments[0].assertion_uuid = wrong; "composite confidence assertion is missing" }
                11 => { subject.knowledge.confidence_assessments[0].provenance_uuid = wrong; "composite confidence provenance is missing" }
                12 => { subject.knowledge.confidence_inputs[0].confidence_uuid = wrong; "composite confidence owner is missing" }
                13 => { subject.knowledge.confidence_inputs[0].input_confidence_uuid = wrong; "composite confidence input is missing" }
                14 => { subject.knowledge.evidence[0].assertion_uuid = wrong; "composite evidence assertion is missing" }
                15 => { subject.knowledge.evidence[0].provenance_uuid = wrong; "composite evidence provenance is missing" }
                16 => {
                    subject.knowledge.reasoning[0].assertion_uuid = wrong;
                    subject.knowledge.reasoning[0].supersedes_reasoning_uuid = None;
                    "composite reasoning assertion is missing"
                }
                17 => { subject.knowledge.reasoning[0].supersedes_reasoning_uuid = Some(wrong); "composite prior reasoning is missing" }
                18 => { subject.knowledge.reasoning[0].provenance_uuid = wrong; "composite reasoning provenance is missing" }
                _ => unreachable!(),
            };
            if case <= 7 {
                let row = &subject.knowledge.lineage[0];
                subject.knowledge.lineage[0] = gf_provenance::LineageRecord::new(
                    row.provenance_uuid, row.subject_uuid, row.subject_kind, row.role, row.ordinal,
                ).unwrap();
            }
            let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
            assert_reference_error(subject.validate_provenance_assertion_references(&snapshot, &identities), ApiErrorCode::NotFound, expected);
        }
        let mut context = reference_request(); let row = &context.knowledge.provenance_events[0];
        context.knowledge.provenance_events[0] = gf_provenance::ProvenanceEvent::new(wrong, row.event_kind, row.actor_uuid, row.recorded_at_micros).unwrap();
        let snapshot = CompositeValidationSnapshot::default(); let identities = context.validate_ontology_and_identities(&snapshot).unwrap();
        assert_reference_error(context.validate_provenance_assertion_references(&snapshot, &identities), ApiErrorCode::IdentityConflict, "composite provenance identity does not match request context");
        let mut actor = reference_request(); let row = &actor.knowledge.provenance_events[0];
        actor.knowledge.provenance_events[0] = gf_provenance::ProvenanceEvent::new(row.operation_uuid, row.event_kind, Some(wrong), row.recorded_at_micros).unwrap();
        let identities = actor.validate_ontology_and_identities(&snapshot).unwrap();
        assert_reference_error(actor.validate_provenance_assertion_references(&snapshot, &identities), ApiErrorCode::IdentityConflict, "composite provenance identity does not match request context");

        let mut precedence = reference_request();
        precedence.knowledge.lineage[0].provenance_uuid = wrong;
        precedence.knowledge.assertions[0].provenance_uuid = wrong;
        let row = &precedence.knowledge.lineage[0];
        precedence.knowledge.lineage[0] = gf_provenance::LineageRecord::new(
            row.provenance_uuid, row.subject_uuid, row.subject_kind, row.role, row.ordinal,
        ).unwrap();
        let identities = precedence.validate_ontology_and_identities(&snapshot).unwrap();
        assert_reference_error(precedence.validate_provenance_assertion_references(&snapshot, &identities), ApiErrorCode::NotFound, "composite lineage provenance is missing");

        let mut field_order = reference_request();
        field_order.knowledge.confidence_assessments[0].assertion_uuid = wrong;
        field_order.knowledge.confidence_assessments[0].provenance_uuid = wrong;
        let identities = field_order.validate_ontology_and_identities(&snapshot).unwrap();
        assert_reference_error(field_order.validate_provenance_assertion_references(&snapshot, &identities), ApiErrorCode::NotFound, "composite confidence assertion is missing");
    }

    #[test]
    fn graph_references_accept_snapshot_existing_objects() {
        let nodes = [uuid7(210), uuid7(211)];
        let edge_uuid = uuid7(212);
        let snapshot = CompositeValidationSnapshot {
            nodes: BTreeSet::from(nodes),
            edges: BTreeSet::from([edge_uuid]),
            ..CompositeValidationSnapshot::default()
        };
        let mut subject = request(vec![edge(uuid7(213), nodes[0], nodes[1])]);
        subject
            .graph_mutations
            .extend(target_mutations(nodes[0], edge_uuid));
        subject.knowledge.assertion_graph_refs = vec![
            graph_ref(nodes[1], GraphObjectKind::Node),
            graph_ref(edge_uuid, GraphObjectKind::Edge),
        ];
        let identities = subject.collect_identities(&snapshot).unwrap();
        assert!(
            subject
                .validate_graph_references(&snapshot, &identities)
                .is_ok()
        );
    }

    #[test]
    fn graph_references_accept_every_same_request_forward_target() {
        let node_uuid = uuid7(214);
        let edge_uuid = uuid7(215);
        let mut mutations = vec![edge(edge_uuid, node_uuid, node_uuid)];
        mutations.extend(target_mutations(node_uuid, edge_uuid));
        mutations.push(node(node_uuid, "Person"));
        let mut subject = request(mutations);
        subject.knowledge.assertion_graph_refs = vec![
            graph_ref(node_uuid, GraphObjectKind::Node),
            graph_ref(edge_uuid, GraphObjectKind::Edge),
        ];
        let snapshot = CompositeValidationSnapshot::default();
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert!(
            subject
                .validate_graph_references(&snapshot, &identities)
                .is_ok()
        );
    }

    #[test]
    fn edge_endpoint_errors_are_source_first_and_field_specific() {
        let node_uuid = uuid7(220);
        let edge_uuid = uuid7(221);
        let missing = uuid7(222);
        let snapshot = CompositeValidationSnapshot {
            nodes: BTreeSet::from([node_uuid]),
            edges: BTreeSet::from([edge_uuid]),
            ..CompositeValidationSnapshot::default()
        };
        let cases = [
            (
                missing,
                node_uuid,
                "composite edge source_uuid does not resolve to a node",
            ),
            (
                edge_uuid,
                node_uuid,
                "composite edge source_uuid does not resolve to a node",
            ),
            (
                node_uuid,
                missing,
                "composite edge target_uuid does not resolve to a node",
            ),
            (
                node_uuid,
                edge_uuid,
                "composite edge target_uuid does not resolve to a node",
            ),
            (
                missing,
                missing,
                "composite edge source_uuid does not resolve to a node",
            ),
        ];
        for (source, target, expected) in cases {
            let subject = request(vec![edge(uuid7(223), source, target)]);
            let identities = subject.collect_identities(&snapshot).unwrap();
            assert_not_found(
                subject.validate_graph_references(&snapshot, &identities),
                expected,
            );
        }
    }

    #[test]
    fn epistemic_forward_and_optional_references_are_order_independent() {
        let mut subject = epistemic_request();
        subject.knowledge.assertions.reverse();
        subject.knowledge.confidence_assessments.reverse();
        subject.knowledge.reasoning.reverse();
        subject.knowledge.assertion_status.reverse();
        subject.knowledge.hypothesis_groups.reverse();
        let snapshot = CompositeValidationSnapshot::default();
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        subject
            .validate_epistemic_event_references(&snapshot, &identities)
            .unwrap();

        for row in &mut subject.knowledge.assertion_status {
            row.confidence_uuid = None;
            row.reasoning_uuid = None;
        }
        for row in &mut subject.knowledge.hypothesis_selection {
            row.selected_assertion_uuid = None;
        }
        for row in &mut subject.knowledge.assertion_validity {
            row.reasoning_uuid = None;
        }
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        subject
            .validate_epistemic_event_references(&snapshot, &identities)
            .unwrap();
    }

    #[test]
    #[rustfmt::skip]
    fn epistemic_references_accept_the_immutable_snapshot() {
        let mut subject = epistemic_request();
        let mut snapshot = CompositeValidationSnapshot::default();
        let provenance = uuid7(180); let assertion = uuid7(181); let replacement = uuid7(182);
        let confidence = uuid7(183); let reasoning = uuid7(184); let status = uuid7(185); let group = uuid7(186);
        snapshot.provenance.insert(provenance); snapshot.assertions.extend([assertion, replacement]);
        snapshot.confidence.insert(confidence); snapshot.reasoning.insert(reasoning);
        snapshot.status_events.insert(status); snapshot.hypothesis_groups.insert(group);
        subject.knowledge.assertion_status.truncate(1);
        let row = &mut subject.knowledge.assertion_status[0];
        row.assertion_uuid = assertion; row.confidence_uuid = Some(confidence);
        row.reasoning_uuid = Some(reasoning); row.provenance_uuid = provenance;
        subject.knowledge.assertion_supersessions.truncate(1);
        let row = &mut subject.knowledge.assertion_supersessions[0];
        row.prior_assertion_uuid = assertion; row.replacement_assertion_uuid = replacement;
        row.status_event_uuid = status; row.reasoning_uuid = reasoning; row.provenance_uuid = provenance;
        subject.knowledge.hypothesis_groups.truncate(1);
        subject.knowledge.hypothesis_groups[0].provenance_uuid = provenance;
        subject.knowledge.hypothesis_membership.truncate(1);
        let row = &mut subject.knowledge.hypothesis_membership[0];
        row.group_uuid = group; row.assertion_uuid = assertion; row.reasoning_uuid = reasoning; row.provenance_uuid = provenance;
        subject.knowledge.hypothesis_selection.truncate(1);
        let row = &mut subject.knowledge.hypothesis_selection[0];
        row.group_uuid = group; row.selected_assertion_uuid = Some(assertion); row.reasoning_uuid = reasoning; row.provenance_uuid = provenance;
        subject.knowledge.assertion_validity.truncate(1);
        let row = &mut subject.knowledge.assertion_validity[0];
        row.assertion_uuid = assertion; row.reasoning_uuid = Some(reasoning); row.provenance_uuid = provenance;
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        subject.validate_epistemic_event_references(&snapshot, &identities).unwrap();
    }

    #[test]
    #[rustfmt::skip]
    fn every_epistemic_reference_rejects_a_wrong_identity_family() {
        let wrong = uuid7(240);
        for case in 0..23 {
            let mut subject = epistemic_request();
            let mut snapshot = CompositeValidationSnapshot::default();
            snapshot.nodes.insert(wrong);
            let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
            let (code, expected) = match case {
                0 => { subject.knowledge.assertion_status[0].assertion_uuid = wrong; (ApiErrorCode::NotFound, "composite status assertion is missing") }
                1 => { subject.knowledge.assertion_status[0].confidence_uuid = Some(wrong); (ApiErrorCode::NotFound, "composite status confidence is missing") }
                2 => { subject.knowledge.assertion_status[0].reasoning_uuid = Some(wrong); (ApiErrorCode::NotFound, "composite status reasoning is missing") }
                3 => { subject.knowledge.assertion_status[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite status provenance is missing") }
                4 => { subject.knowledge.assertion_supersessions[0].prior_assertion_uuid = wrong; (ApiErrorCode::NotFound, "composite prior assertion is missing") }
                5 => { subject.knowledge.assertion_supersessions[0].replacement_assertion_uuid = wrong; (ApiErrorCode::NotFound, "composite replacement assertion is missing") }
                6 => { subject.knowledge.assertion_supersessions[0].status_event_uuid = wrong; (ApiErrorCode::NotFound, "composite supersession status is missing") }
                7 => { subject.knowledge.assertion_supersessions[0].reasoning_uuid = wrong; (ApiErrorCode::NotFound, "composite supersession reasoning is missing") }
                8 => { subject.knowledge.assertion_supersessions[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite supersession provenance is missing") }
                9 => { subject.knowledge.hypothesis_groups[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite hypothesis provenance is missing") }
                10 => { subject.knowledge.hypothesis_membership[0].operation_uuid = wrong; (ApiErrorCode::IdentityConflict, "composite participant operation does not match request identity") }
                11 => { subject.knowledge.hypothesis_membership[0].group_uuid = wrong; (ApiErrorCode::NotFound, "composite membership group is missing") }
                12 => { subject.knowledge.hypothesis_membership[0].assertion_uuid = wrong; (ApiErrorCode::NotFound, "composite membership assertion is missing") }
                13 => { subject.knowledge.hypothesis_membership[0].reasoning_uuid = wrong; (ApiErrorCode::NotFound, "composite membership reasoning is missing") }
                14 => { subject.knowledge.hypothesis_membership[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite membership provenance is missing") }
                15 => { subject.knowledge.hypothesis_selection[0].operation_uuid = wrong; (ApiErrorCode::IdentityConflict, "composite participant operation does not match request identity") }
                16 => { subject.knowledge.hypothesis_selection[0].group_uuid = wrong; (ApiErrorCode::NotFound, "composite selection group is missing") }
                17 => { subject.knowledge.hypothesis_selection[0].selected_assertion_uuid = Some(wrong); (ApiErrorCode::NotFound, "composite selected assertion is missing") }
                18 => { subject.knowledge.hypothesis_selection[0].reasoning_uuid = wrong; (ApiErrorCode::NotFound, "composite selection reasoning is missing") }
                19 => { subject.knowledge.hypothesis_selection[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite selection provenance is missing") }
                20 => { subject.knowledge.assertion_validity[0].assertion_uuid = wrong; (ApiErrorCode::NotFound, "composite validity assertion is missing") }
                21 => { subject.knowledge.assertion_validity[0].reasoning_uuid = Some(wrong); (ApiErrorCode::NotFound, "composite validity reasoning is missing") }
                22 => { subject.knowledge.assertion_validity[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite validity provenance is missing") }
                _ => unreachable!(),
            };
            assert_reference_error(subject.validate_epistemic_event_references(&snapshot, &identities), code, expected);
        }
    }

    #[test]
    fn mutation_targets_reject_missing_and_wrong_kinds_for_every_form() {
        let missing = uuid7(224);
        let wrong_node = uuid7(225);
        let wrong_edge = uuid7(226);
        let snapshot = CompositeValidationSnapshot {
            nodes: BTreeSet::from([wrong_edge]),
            edges: BTreeSet::from([wrong_node]),
            ..CompositeValidationSnapshot::default()
        };
        for (node_uuid, edge_uuid) in [(missing, missing), (wrong_node, wrong_edge)] {
            for (index, mutation) in target_mutations(node_uuid, edge_uuid)
                .into_iter()
                .enumerate()
            {
                let subject = request(vec![mutation]);
                let identities = subject.collect_identities(&snapshot).unwrap();
                let expected = if index < 3 {
                    "composite node mutation target does not resolve to a node"
                } else {
                    "composite edge mutation target does not resolve to an edge"
                };
                assert_not_found(
                    subject.validate_graph_references(&snapshot, &identities),
                    expected,
                );
            }
        }
    }

    #[test]
    fn declared_graph_references_reject_missing_and_wrong_kinds() {
        let missing = uuid7(227);
        let existing = uuid7(228);
        let cases = [
            (
                graph_ref(missing, GraphObjectKind::Node),
                CompositeValidationSnapshot::default(),
            ),
            (
                graph_ref(missing, GraphObjectKind::Edge),
                CompositeValidationSnapshot::default(),
            ),
            (
                graph_ref(existing, GraphObjectKind::Node),
                CompositeValidationSnapshot {
                    edges: BTreeSet::from([existing]),
                    ..CompositeValidationSnapshot::default()
                },
            ),
            (
                graph_ref(existing, GraphObjectKind::Edge),
                CompositeValidationSnapshot {
                    nodes: BTreeSet::from([existing]),
                    ..CompositeValidationSnapshot::default()
                },
            ),
        ];
        for (row, snapshot) in cases {
            let mut subject = request(Vec::new());
            subject.knowledge.assertion_graph_refs.push(row);
            let identities = subject.collect_identities(&snapshot).unwrap();
            assert_not_found(
                subject.validate_graph_references(&snapshot, &identities),
                "composite graph reference does not resolve to its declared kind",
            );
        }
    }

    #[test]
    #[rustfmt::skip]
    fn epistemic_family_and_field_precedence_is_stable() {
        let wrong = uuid7(240); let snapshot = CompositeValidationSnapshot::default();
        for case in 0..6 {
            let mut subject = epistemic_request();
            let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
            let (code, expected) = match case {
                0 => { let row = &mut subject.knowledge.assertion_status[0]; row.assertion_uuid = wrong; row.confidence_uuid = Some(wrong); row.reasoning_uuid = Some(wrong); row.provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite status assertion is missing") }
                1 => { let row = &mut subject.knowledge.assertion_supersessions[0]; row.prior_assertion_uuid = wrong; row.replacement_assertion_uuid = wrong; row.status_event_uuid = wrong; row.reasoning_uuid = wrong; row.provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite prior assertion is missing") }
                2 => { subject.knowledge.hypothesis_groups[0].provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite hypothesis provenance is missing") }
                3 => { let row = &mut subject.knowledge.hypothesis_membership[0]; row.operation_uuid = wrong; row.group_uuid = wrong; row.assertion_uuid = wrong; row.reasoning_uuid = wrong; row.provenance_uuid = wrong; (ApiErrorCode::IdentityConflict, "composite participant operation does not match request identity") }
                4 => { let row = &mut subject.knowledge.hypothesis_selection[0]; row.operation_uuid = wrong; row.group_uuid = wrong; row.selected_assertion_uuid = Some(wrong); row.reasoning_uuid = wrong; row.provenance_uuid = wrong; (ApiErrorCode::IdentityConflict, "composite participant operation does not match request identity") }
                5 => { let row = &mut subject.knowledge.assertion_validity[0]; row.assertion_uuid = wrong; row.reasoning_uuid = Some(wrong); row.provenance_uuid = wrong; (ApiErrorCode::NotFound, "composite validity assertion is missing") }
                _ => unreachable!(),
            };
            assert_reference_error(subject.validate_epistemic_event_references(&snapshot, &identities), code, expected);
        }
    }

    #[test]
    fn participant_references_accept_aligned_provenance_families() {
        let snapshot = CompositeValidationSnapshot::default();
        let subject = reference_request();
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        subject
            .validate_participant_references(&snapshot, &identities)
            .unwrap();
    }

    #[test]
    fn participant_references_run_provenance_before_epistemic() {
        let wrong = uuid7(241);
        let snapshot = CompositeValidationSnapshot::default();
        let mut subject = reference_request();
        // Keep one epistemic participant so both families are present, then
        // introduce simultaneous provenance and epistemic defects.
        subject.knowledge.assertion_status =
            crate::composite_transaction::tests::full_knowledge_fixture().assertion_status;
        subject.knowledge.assertion_status.truncate(1);
        let status = &mut subject.knowledge.assertion_status[0];
        status.assertion_uuid = subject.knowledge.assertions[0].assertion_uuid;
        status.provenance_uuid = subject.knowledge.provenance_events[0].provenance_uuid;
        status.confidence_uuid = None;
        status.reasoning_uuid = None;

        subject.knowledge.assertions[0].provenance_uuid = wrong;
        status.assertion_uuid = wrong;
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert_not_found(
            subject.validate_participant_references(&snapshot, &identities),
            "composite assertion provenance is missing",
        );
    }

    #[test]
    fn participant_references_surface_epistemic_defects_after_provenance() {
        let wrong = uuid7(242);
        let snapshot = CompositeValidationSnapshot::default();
        let mut subject = reference_request();
        subject.knowledge.assertion_status =
            crate::composite_transaction::tests::full_knowledge_fixture().assertion_status;
        subject.knowledge.assertion_status.truncate(1);
        let status = &mut subject.knowledge.assertion_status[0];
        status.assertion_uuid = wrong;
        status.provenance_uuid = subject.knowledge.provenance_events[0].provenance_uuid;
        status.confidence_uuid = None;
        status.reasoning_uuid = None;
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert_not_found(
            subject.validate_participant_references(&snapshot, &identities),
            "composite status assertion is missing",
        );
    }

    #[test]
    fn graph_and_participant_references_accept_aligned_request() {
        let snapshot = CompositeValidationSnapshot::default();
        let subject = reference_request();
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        subject
            .validate_graph_and_participant_references(&snapshot, &identities)
            .unwrap();
    }

    #[test]
    fn graph_and_participant_references_run_graph_before_participant() {
        let wrong = uuid7(243);
        let snapshot = CompositeValidationSnapshot::default();
        let mut subject = reference_request();
        // Simultaneous graph-kind and provenance defects: aggregate must
        // surface the graph-family first error.
        subject.knowledge.assertion_graph_refs[0].graph_kind = GraphObjectKind::Edge;
        subject.knowledge.assertions[0].provenance_uuid = wrong;
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert_not_found(
            subject.validate_graph_and_participant_references(&snapshot, &identities),
            "composite graph reference does not resolve to its declared kind",
        );
    }

    #[test]
    fn graph_and_participant_references_surface_participant_after_graph() {
        let wrong = uuid7(244);
        let snapshot = CompositeValidationSnapshot::default();
        let mut subject = reference_request();
        subject.knowledge.assertions[0].provenance_uuid = wrong;
        let identities = subject.validate_ontology_and_identities(&snapshot).unwrap();
        assert_not_found(
            subject.validate_graph_and_participant_references(&snapshot, &identities),
            "composite assertion provenance is missing",
        );
    }

    /// Pre-staging validation phases in documented order (#2660 / #2599).
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ValidationPhase {
        RequestDomain,
        Ontology,
        Identities,
        GraphReferences,
        ParticipantReferences,
    }

    #[derive(Debug)]
    struct MutationProbe {
        staging_calls: std::cell::Cell<u32>,
        participant_writes: std::cell::Cell<u32>,
        generation: std::cell::Cell<u64>,
    }

    impl MutationProbe {
        fn fresh() -> Self {
            Self {
                staging_calls: std::cell::Cell::new(0),
                participant_writes: std::cell::Cell::new(0),
                generation: std::cell::Cell::new(1),
            }
        }

        fn assert_untouched(&self) {
            assert_eq!(self.staging_calls.get(), 0);
            assert_eq!(self.participant_writes.get(), 0);
            assert_eq!(self.generation.get(), 1);
        }

        fn run(
            &self,
            request: &CompositeTransactionRequest,
            snapshot: &CompositeValidationSnapshot,
        ) -> Result<(), GfError> {
            match validate_pre_staging(request, snapshot) {
                Ok(()) => {
                    self.staging_calls.set(self.staging_calls.get() + 1);
                    self.participant_writes
                        .set(self.participant_writes.get() + 1);
                    self.generation.set(self.generation.get() + 1);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
    }

    fn validate_pre_staging(
        request: &CompositeTransactionRequest,
        snapshot: &CompositeValidationSnapshot,
    ) -> Result<(), GfError> {
        let identities = request.validate_ontology_and_identities(snapshot)?;
        request.validate_graph_and_participant_references(snapshot, &identities)
    }

    fn assert_pre_staging_error(error: GfError, expected_code: &str, expected_message: &str) {
        assert_eq!(error.code(), expected_code);
        let rendered = error.to_string();
        assert!(
            rendered.contains(expected_message),
            "expected {expected_message:?} in {rendered}"
        );
        assert!(!rendered.contains("private-"));
    }

    /// Inject later-phase defects so multi-defect rows prove earliest-phase wins.
    fn inject_later_phase_defects(
        request: &mut CompositeTransactionRequest,
        snapshot: &mut CompositeValidationSnapshot,
        from: ValidationPhase,
    ) {
        let wrong = uuid7(250);
        if matches!(
            from,
            ValidationPhase::RequestDomain
                | ValidationPhase::Ontology
                | ValidationPhase::Identities
                | ValidationPhase::GraphReferences
                | ValidationPhase::ParticipantReferences
        ) {
            // Participant defect (last phase).
            if !request.knowledge.assertions.is_empty() {
                request.knowledge.assertions[0].provenance_uuid = wrong;
            }
        }
        if matches!(
            from,
            ValidationPhase::RequestDomain
                | ValidationPhase::Ontology
                | ValidationPhase::Identities
                | ValidationPhase::GraphReferences
        ) {
            // Graph defect.
            if !request.knowledge.assertion_graph_refs.is_empty() {
                request.knowledge.assertion_graph_refs[0].graph_kind = GraphObjectKind::Edge;
            }
        }
        if matches!(
            from,
            ValidationPhase::RequestDomain
                | ValidationPhase::Ontology
                | ValidationPhase::Identities
        ) {
            // Identity collision against an existing node.
            let collision = uuid7(251);
            snapshot.nodes.insert(collision);
            request.graph_mutations.push(node(collision, "Person"));
        }
        if matches!(
            from,
            ValidationPhase::RequestDomain | ValidationPhase::Ontology
        ) {
            snapshot.ontology.mode = OntologyMode::Strict;
            // Leave entity type undeclared so ontology fails when reached.
        }
    }

    #[test]
    fn composite_validation_precedence_ledger_and_zero_mutation_proof() {
        #[derive(Clone, Copy)]
        struct Case {
            phase: ValidationPhase,
            code: &'static str,
            message: &'static str,
        }

        let ledger = [
            Case {
                phase: ValidationPhase::RequestDomain,
                code: "GF_VALIDATION",
                message: "composite request has an unsupported contract version",
            },
            Case {
                phase: ValidationPhase::Ontology,
                code: "GF_ONTOLOGY",
                message: "composite request uses an undeclared entity type",
            },
            Case {
                phase: ValidationPhase::Identities,
                code: "GF_IDENTITY_CONFLICT",
                message: "composite request reuses an occupied identity",
            },
            Case {
                phase: ValidationPhase::GraphReferences,
                code: "GF_NOT_FOUND",
                message: "composite graph reference does not resolve to its declared kind",
            },
            Case {
                phase: ValidationPhase::ParticipantReferences,
                code: "GF_NOT_FOUND",
                message: "composite assertion provenance is missing",
            },
        ];

        for case in ledger {
            let probe = MutationProbe::fresh();
            let mut snapshot = CompositeValidationSnapshot::default();
            let mut subject = reference_request();
            inject_later_phase_defects(&mut subject, &mut snapshot, case.phase);

            match case.phase {
                ValidationPhase::RequestDomain => {
                    subject.contract_version = u32::MAX;
                }
                ValidationPhase::Ontology => {
                    snapshot.ontology.mode = OntologyMode::Strict;
                    snapshot.ontology.entity_types.clear();
                    snapshot.ontology.relation_types.clear();
                    // Remove the identity collision inject_later may have added so
                    // ontology is the earliest remaining failure.
                    subject.graph_mutations.retain(|mutation| {
                        !matches!(
                            mutation,
                            CompositeGraphMutation::CreateNode { node_uuid, .. }
                                if *node_uuid == uuid7(251)
                        )
                    });
                    snapshot.nodes.remove(&uuid7(251));
                }
                ValidationPhase::Identities => {
                    // Keep collision from inject_later; clear ontology strictness.
                    snapshot.ontology.mode = OntologyMode::Exploratory;
                }
                ValidationPhase::GraphReferences => {
                    snapshot.ontology.mode = OntologyMode::Exploratory;
                    subject.graph_mutations.retain(|mutation| {
                        !matches!(
                            mutation,
                            CompositeGraphMutation::CreateNode { node_uuid, .. }
                                if *node_uuid == uuid7(251)
                        )
                    });
                    snapshot.nodes.remove(&uuid7(251));
                    // Keep graph kind defect; restore participant so graph wins.
                    let provenance = subject.knowledge.provenance_events[0].provenance_uuid;
                    subject.knowledge.assertions[0].provenance_uuid = provenance;
                }
                ValidationPhase::ParticipantReferences => {
                    snapshot.ontology.mode = OntologyMode::Exploratory;
                    subject.graph_mutations.retain(|mutation| {
                        !matches!(
                            mutation,
                            CompositeGraphMutation::CreateNode { node_uuid, .. }
                                if *node_uuid == uuid7(251)
                        )
                    });
                    snapshot.nodes.remove(&uuid7(251));
                    if !subject.knowledge.assertion_graph_refs.is_empty() {
                        subject.knowledge.assertion_graph_refs[0].graph_kind =
                            GraphObjectKind::Node;
                        subject.knowledge.assertion_graph_refs[0].graph_uuid = uuid7(230);
                    }
                }
            }

            let error = probe.run(&subject, &snapshot).unwrap_err();
            assert_pre_staging_error(error, case.code, case.message);
            probe.assert_untouched();
        }

        // Same-request forward references succeed regardless of mutation order.
        let probe = MutationProbe::fresh();
        let snapshot = CompositeValidationSnapshot::default();
        let source = uuid7(160);
        let target = uuid7(161);
        let mut forward = request(vec![
            edge(uuid7(162), source, target),
            node(source, "Person"),
            node(target, "Person"),
        ]);
        // Keep knowledge empty so only graph same-request resolution is exercised.
        forward.knowledge = CompositeKnowledgeParticipants::default();
        probe.run(&forward, &snapshot).unwrap();
        assert_eq!(probe.staging_calls.get(), 1);
        assert_eq!(probe.participant_writes.get(), 1);
        assert_eq!(probe.generation.get(), 2);

        let mut reversed = request(vec![
            node(target, "Person"),
            node(source, "Person"),
            edge(uuid7(162), source, target),
        ]);
        reversed.knowledge = CompositeKnowledgeParticipants::default();
        let probe = MutationProbe::fresh();
        probe.run(&reversed, &snapshot).unwrap();
        assert_eq!(probe.staging_calls.get(), 1);
    }
}
