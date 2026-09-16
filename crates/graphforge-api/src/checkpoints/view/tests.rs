use super::*;
use crate::checkpoints::tests::{operation, uuid7};
use crate::checkpoints::{CheckpointRequest, DeleteCheckpointRequest};
use arrow::array::Array;
use arrow::array::StringArray;
use graphforge_core::OntologyMode;
use tempfile::tempdir;

#[test]
fn checkpoint_view_stays_pinned_and_rejects_writes() {
    let directory = tempdir().unwrap();
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    let source = graph.add_node("Endpoint", &HashMap::new()).unwrap();
    let target = graph.add_node("Endpoint", &HashMap::new()).unwrap();
    graph.execute("CREATE (:Person {name: 'before'})").unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "Before".into(),
            description: Some("stable view".into()),
            idempotency_key: operation(1),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Person {name: 'after'})").unwrap();

    let mut view = graph.open_checkpoint("Before").unwrap();
    let result = view
        .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .unwrap();
    let names = result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names.value(0), "before");
    assert_eq!(
        view.inspect_adjacency().unwrap().state,
        graphforge_storage::adjacency::AdjacencyFreshnessState::Missing
    );
    assert_eq!(
        view.execute("CREATE (:Person)").unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );

    graph
        .delete_checkpoint(DeleteCheckpointRequest {
            name: "Before".into(),
            idempotency_key: operation(2),
            actor_uuid: None,
        })
        .unwrap();
    assert_eq!(
        graph.open_checkpoint("Before").unwrap_err().code(),
        "GF_CHECKPOINT_NOT_FOUND"
    );
    std::fs::write(directory.path().join("CURRENT"), b"invalid\n").unwrap();
    assert!(!view.project_capabilities().unwrap().batches.is_empty());
    assert_eq!(
        view.enable_capability(crate::EnableCapabilityRequest {
            context: crate::WriteContext {
                operation_uuid: operation(3),
                actor_uuid: None,
            },
            capability_id: crate::CapabilityId::Knowledge,
            capability_version: 1,
        })
        .unwrap_err()
        .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.add_node("Person", &HashMap::new()).unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.add_edge(&source, "LINK", &target, &HashMap::new())
            .unwrap_err()
            .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.index_adjacency().unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.index_search(
            "Person",
            crate::SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap_err()
        .code(),
        "GF_READ_ONLY_VIEW"
    );
    for error in [
        view.checkpoint(CheckpointRequest {
            name: "Nested".into(),
            description: None,
            idempotency_key: operation(4),
            actor_uuid: None,
        })
        .unwrap_err(),
        view.delete_checkpoint(DeleteCheckpointRequest {
            name: "Missing".into(),
            idempotency_key: operation(5),
            actor_uuid: None,
        })
        .unwrap_err(),
    ] {
        assert_eq!(error.code(), "GF_READ_ONLY_VIEW");
    }
    assert_eq!(
        view.bind_embedding_space_alias("alias", "space", false)
            .unwrap_err()
            .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.remove_embedding_space_alias("alias")
            .unwrap_err()
            .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.delete_embedding_space(Some("space"))
            .unwrap_err()
            .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.set_default_embedding_space(None).unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.adopt_ontology(crate::AdoptOntologyRequest {
            context: crate::WriteContext {
                operation_uuid: operation(6),
                actor_uuid: None,
            },
            path: directory.path().join("unused.yaml"),
            mode: OntologyMode::Strict,
        })
        .unwrap_err()
        .code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.clear_ontology(crate::ClearOntologyRequest {
            context: crate::WriteContext {
                operation_uuid: operation(7),
                actor_uuid: None,
            },
        })
        .unwrap_err()
        .code(),
        "GF_READ_ONLY_VIEW"
    );
    let context = |seed| crate::WriteContext {
        operation_uuid: operation(seed),
        actor_uuid: None,
    };
    let assertion = |seed| crate::CreateAssertionRequest {
        context: context(seed),
        assertion_uuid: uuid7(seed as u8),
        claim: format!("checkpoint view assertion {seed}"),
        graph_refs: Vec::new(),
    };
    let assertion_uuid = uuid7(20);
    let provenance_uuid = uuid7(21);
    let reasoning_uuid = uuid7(22);
    let group_uuid = uuid7(23);
    let knowledge_errors = [
        view.create_assertion(assertion(20)).unwrap_err(),
        view.create_assertion_with_status(crate::CreateAssertionWithStatusRequest {
            assertion: assertion(21),
            first_status: crate::FirstAssertionStatusInput {
                status_event_uuid: uuid7(24),
                status: graphforge_knowledge::AssertionStatus::Hypothesis,
            },
        })
        .unwrap_err(),
        view.create_assertion_with_evidence(crate::CreateAssertionWithEvidenceRequest {
            assertion: assertion(22),
            evidence: vec![crate::EvidenceInput {
                evidence_uuid: uuid7(25),
                source_uuid: uuid7(26),
                source_kind: graphforge_knowledge::EvidenceSourceKind::Document,
                role: graphforge_knowledge::EvidenceRole::Context,
                weight: None,
            }],
        })
        .unwrap_err(),
        view.assess_confidence(crate::AssessConfidenceRequest {
            context: context(23),
            confidence_uuid: uuid7(27),
            assertion_uuid,
            policy: crate::ConfidencePolicyRequest::Explicit { value: 0.5 },
        })
        .unwrap_err(),
        view.attach_evidence(crate::AttachEvidenceRequest {
            context: context(24),
            evidence_uuid: uuid7(28),
            assertion_uuid,
            source_uuid: uuid7(29),
            source_kind: graphforge_knowledge::EvidenceSourceKind::Observation,
            role: graphforge_knowledge::EvidenceRole::Supports,
            weight: Some(0.75),
        })
        .unwrap_err(),
        view.record_reasoning(crate::RecordReasoningRequest {
            context: context(25),
            reasoning_uuid,
            assertion_uuid,
            kind: graphforge_knowledge::ReasoningKind::DecisionRationale,
            content_format: graphforge_knowledge::ReasoningContentFormat::TextPlain,
            content: b"immutable checkpoint".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap_err(),
        view.record_assertion_status(crate::RecordAssertionStatusRequest {
            context: context(26),
            status_event_uuid: uuid7(30),
            assertion_uuid,
            status: graphforge_knowledge::AssertionStatus::Supported,
            confidence_uuid: None,
            reasoning_uuid: Some(reasoning_uuid),
            provenance_uuid,
        })
        .unwrap_err(),
        view.supersede_assertion(crate::SupersedeAssertionRequest {
            context: context(27),
            supersession_uuid: uuid7(31),
            prior_assertion_uuid: assertion_uuid,
            replacement_assertion_uuid: uuid7(32),
            status_event_uuid: uuid7(33),
            reasoning_uuid,
            provenance_uuid,
        })
        .unwrap_err(),
        view.create_hypothesis_group(crate::CreateHypothesisGroupRequest {
            context: context(28),
            group_uuid,
            question_key: "checkpoint.view.v1".into(),
            provenance_uuid,
        })
        .unwrap_err(),
        view.record_hypothesis_membership(&crate::RecordHypothesisMembershipRequest {
            context: context(29),
            membership_event_uuid: uuid7(34),
            group_uuid,
            assertion_uuid,
            action: graphforge_knowledge::HypothesisMembershipAction::Added,
            reasoning_uuid,
            provenance_uuid,
        })
        .unwrap_err(),
        view.record_hypothesis_selection(&crate::RecordHypothesisSelectionRequest {
            context: context(30),
            selection_event_uuid: uuid7(35),
            group_uuid,
            selected_assertion_uuid: Some(assertion_uuid),
            reasoning_uuid,
            provenance_uuid,
        })
        .unwrap_err(),
        view.remove_hypothesis_member(&crate::RemoveHypothesisMemberRequest {
            context: context(31),
            membership_event_uuid: uuid7(36),
            selection_event_uuid: uuid7(37),
            group_uuid,
            assertion_uuid,
            selected_assertion_uuid: None,
            reasoning_uuid,
            provenance_uuid,
        })
        .unwrap_err(),
        view.record_assertion_validity(crate::RecordAssertionValidityRequest {
            context: context(32),
            validity_event_uuid: uuid7(38),
            assertion_uuid,
            valid_from_micros: Some(100),
            valid_to_micros: Some(200),
            reasoning_uuid: Some(reasoning_uuid),
            provenance_uuid,
        })
        .unwrap_err(),
    ];
    for error in knowledge_errors {
        assert_eq!(error.code(), "GF_READ_ONLY_VIEW");
        assert_eq!(
            error.to_string(),
            "GF_READ_ONLY_VIEW: checkpoint views are read-only"
        );
    }
    assert_eq!(
        view.execute("MATCH (n) RETURN count(n) AS total")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
}
